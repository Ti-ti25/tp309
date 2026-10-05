"""
Bibliothèque cliente de la messagerie (aucune dépendance graphique).

Cette couche cache les détails de MQTT à l'interface graphique : elle offre un
mécanisme de *requête / réponse asynchrone* au-dessus d'un protocole qui, lui,
ne connaît que des publications sans réponse.

Principe :
  1. chaque requête reçoit un identifiant unique et est publiée sur
     mail/<domaine>/server/request/<client_id> ;
  2. on mémorise (identifiant -> fonction callback) et on arme un
     threading.Timer de garde ;
  3. quand la réponse arrive sur mail/<domaine>/client/<client_id>/response,
     on_message retrouve le callback grâce à l'identifiant et l'appelle ;
  4. si le Timer expire avant, le callback est appelé avec une erreur.

Les autres messages reçus sont des événements (nouveau mail, présence d'un
contact, état du serveur) transmis à l'application via des callbacks
"on_..." qu'elle peut redéfinir.

ATTENTION : tous ces callbacks sont appelés depuis le thread réseau de paho
(ou d'un Timer), jamais depuis le thread Tkinter. L'IHM doit donc les
retransmettre à son propre thread (voir gui_client.py).
"""

import mimetypes
import os
import socket
import struct
import threading
import time

import paho.mqtt.client as mqtt

from . import protocole as P


# ======================================================================
# Utilitaires réseau "bas niveau" (sockets TCP/UDP)
# ======================================================================
def ip_locale(hote_distant="8.8.8.8", port=80):
    """
    Adresse IP de l'interface utilisée pour joindre le broker.
    Astuce classique : un socket UDP "connecté" ne transmet rien mais le
    système choisit l'interface de sortie, que l'on lit avec getsockname().
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((hote_distant, port))
            return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def sonder_broker(hote, port, delai=3.0):
    """
    Teste un broker MQTT "à la main", avec un simple socket TCP : on construit
    nous-mêmes le paquet CONNECT du protocole MQTT 3.1.1, on lit le CONNACK
    puis on envoie DISCONNECT. Renvoie (ok, message, latence_ms).
    """
    identifiant = f"sonde-{os.getpid()}".encode()
    # En-tête variable : nom du protocole, niveau 4 (3.1.1), drapeaux
    # (clean session), keep-alive de 10 s ; charge utile : client-id.
    variable = struct.pack("!H4sBBH", 4, b"MQTT", 4, 0x02, 10)
    charge = struct.pack("!H", len(identifiant)) + identifiant
    longueur = len(variable) + len(charge)          # < 128 : tient sur 1 octet
    paquet_connect = bytes([0x10, longueur]) + variable + charge
    debut = time.perf_counter()
    try:
        with socket.create_connection((hote, int(port)), timeout=delai) as sock:
            sock.sendall(paquet_connect)
            connack = sock.recv(4)
            latence = (time.perf_counter() - debut) * 1000
            if len(connack) < 4 or connack[0] != 0x20:
                return False, "réponse inattendue (est-ce bien un broker MQTT ?)", latence
            code = connack[3]
            sock.sendall(b"\xe0\x00")                  # paquet DISCONNECT
            if code != 0:
                return False, f"connexion refusée par le broker (code {code})", latence
            return True, "broker MQTT joignable", latence
    except OSError as exc:
        return False, f"broker injoignable : {exc}", None


def charger_piece_jointe(chemin):
    """Lit un fichier local et le prépare pour l'envoi (contenu en base64)."""
    taille = os.path.getsize(chemin)
    if taille > P.TAILLE_MAX_PJ:
        raise ValueError(f"fichier trop volumineux ({P.taille_lisible(taille)}, "
                         f"maximum {P.taille_lisible(P.TAILLE_MAX_PJ)})")
    with open(chemin, "rb") as fichier:
        contenu = fichier.read()
    mime = mimetypes.guess_type(chemin)[0] or "application/octet-stream"
    return {"name": os.path.basename(chemin), "mime": mime, "size": taille,
            "data": P.encoder_pj(contenu)}


# ======================================================================
# Client de messagerie
# ======================================================================
class ClientMessagerie:
    """Connexion d'un utilisateur à un serveur de messagerie via MQTT."""

    def __init__(self, hote="localhost", port=1883, domaine="rt.iut"):
        self.hote, self.port, self.domaine = hote, int(port), domaine
        self.client_id = "cli-" + P.nouvel_id()[:12]
        self.ip = ip_locale(hote if hote not in ("localhost", "127.0.0.1") else "8.8.8.8")
        self.jeton = None
        self.utilisateur = None
        self.adresse = None
        self.profil = None
        self.serveur_en_ligne = None
        self.presences = {}                  # adresse -> dernier état de présence reçu
        self.keepalive = 30                  # période de keep-alive MQTT (s)

        self._en_attente = {}                # id requête -> (callback, Timer)
        self._verrou = threading.Lock()
        self._abonne = threading.Event()
        self._battement = None

        # --- callbacks à redéfinir par l'application ---------------------
        self.on_evenement = lambda evt: None           # dict {"event":..., "data":...}
        self.on_presence = lambda presence: None       # dict (voir serveur.presence)
        self.on_statut_serveur = lambda statut: None   # dict {"online": bool, ...}
        self.on_connexion_mqtt = lambda connecte, msg: None

        self.mqtt = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                                client_id=self.client_id, clean_session=True)
        # Testament : publié par le broker si nous disparaissons sans prévenir
        # (plantage, câble débranché...). Le serveur ferme alors notre session.
        self.mqtt.will_set(P.topic_deconnexion(domaine, self.client_id),
                           P.encoder({"v": P.VERSION, "client_id": self.client_id}), qos=1)
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_subscribe = self._on_subscribe
        self.mqtt.on_disconnect = self._on_disconnect
        self.mqtt.on_message = self._on_message
        self.mqtt.reconnect_delay_set(1, 10)

    # ------------------------------------------------------------------
    # Connexion au broker
    # ------------------------------------------------------------------
    def connecter(self, delai=5.0):
        """Se connecte au broker et attend que les abonnements soient effectifs."""
        self._abonne.clear()
        self.mqtt.connect(self.hote, self.port, keepalive=self.keepalive)  # OSError si injoignable
        self.mqtt.loop_start()
        if not self._abonne.wait(delai):
            self.fermer()
            raise OSError("le broker n'a pas confirmé les abonnements")

    def fermer(self):
        """
        Ferme proprement la connexion MQTT. Une déconnexion propre ne déclenche
        pas le testament : si une session est encore ouverte, on publie donc
        nous-mêmes le message de déconnexion pour que le serveur la ferme.
        """
        self._arreter_battement()
        if self.jeton:
            self.jeton = None
            self.mqtt.publish(P.topic_deconnexion(self.domaine, self.client_id),
                              P.encoder({"v": P.VERSION, "client_id": self.client_id}), qos=1)
        with self._verrou:
            for _, minuterie in self._en_attente.values():
                minuterie.cancel()
            self._en_attente.clear()
        try:
            self.mqtt.disconnect()
            self.mqtt.loop_stop()
        except Exception:
            pass

    @property
    def connecte(self):
        return self.jeton is not None

    # ------------------------------------------------------------------
    # Callbacks MQTT (thread réseau paho)
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.on_connexion_mqtt(False, f"connexion refusée : {reason_code}")
            return
        client.subscribe([(P.topic_client(self.domaine, self.client_id), 1),
                          (P.topic_presences_tous_domaines(), 1),
                          (P.topic_statut_serveur(self.domaine), 1)])
        self.on_connexion_mqtt(True, f"connecté au broker {self.hote}:{self.port}")

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        self._abonne.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            self.on_connexion_mqtt(False, f"connexion au broker perdue ({reason_code})")

    def _on_message(self, client, userdata, msg):
        segments = P.analyser_topic(msg.topic)
        if not segments or not msg.payload:
            return
        try:
            message = P.decoder(msg.payload)
        except ValueError:
            return
        if len(segments) == 4 and segments[2] == "presence":
            self.presences[message.get("address")] = message
            self.on_presence(message)
        elif segments[2:] == ["server", "status"]:
            self.serveur_en_ligne = bool(message.get("online"))
            self.on_statut_serveur(message)
        elif segments[2] == "client" and segments[-1] == "response":
            self._traiter_reponse(message)
        elif segments[2] == "client" and segments[-1] == "event":
            if message.get("event") == P.EVT_SESSION_CLOSED:
                self._session_terminee()
            self.on_evenement(message)

    def _traiter_reponse(self, reponse):
        with self._verrou:
            attente = self._en_attente.pop(reponse.get("id"), None)
        if attente is None:
            return                       # réponse tardive : le Timer a déjà expiré
        callback, minuterie = attente
        minuterie.cancel()
        if callback:
            callback(reponse)

    # ------------------------------------------------------------------
    # Requêtes asynchrones
    # ------------------------------------------------------------------
    def requete(self, type_req, donnees=None, callback=None, delai=P.DELAI_REQUETE):
        """
        Envoie une requête au serveur. callback(reponse) sera appelé plus tard
        (depuis un autre thread) avec un dict contenant "ok" et "data" ou "error".
        """
        req = P.requete(type_req, donnees, self.jeton)
        minuterie = threading.Timer(delai, self._expiration, args=(req["id"],))
        minuterie.daemon = True
        with self._verrou:
            self._en_attente[req["id"]] = (callback, minuterie)
        minuterie.start()
        info = self.mqtt.publish(P.topic_requete(self.domaine, self.client_id),
                                 P.encoder(req), qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self._expiration(req["id"], "non connecté au broker")
        return req["id"]

    def _expiration(self, id_req, message="le serveur ne répond pas (délai dépassé)"):
        with self._verrou:
            attente = self._en_attente.pop(id_req, None)
        if attente:
            attente[1].cancel()
            if attente[0]:
                attente[0]({"ok": False, "error": message, "id": id_req})

    def requete_sync(self, type_req, donnees=None, delai=P.DELAI_REQUETE):
        """Version bloquante de requete() (tests, scripts). NE PAS appeler depuis Tkinter."""
        fini = threading.Event()
        resultat = {}

        def recevoir(reponse):
            resultat.update(reponse)
            fini.set()

        self.requete(type_req, donnees, recevoir, delai)
        fini.wait(delai + 1)
        return resultat or {"ok": False, "error": "délai dépassé"}

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    def inscrire(self, utilisateur, mot_de_passe, nom, prenom, callback=None):
        return self.requete(P.REQ_REGISTER, {"username": utilisateur, "password": mot_de_passe,
                                             "nom": nom, "prenom": prenom}, callback)

    def connexion(self, utilisateur, mot_de_passe, callback=None):
        """Ouvre une session ; en cas de succès le battement de cœur démarre."""
        def recevoir(reponse):
            if reponse.get("ok"):
                donnees = reponse["data"]
                self.jeton = donnees["token"]
                self.adresse = donnees["address"]
                self.utilisateur = self.adresse.split("@")[0]
                self.profil = donnees.get("profile")
                self._armer_battement()
            if callback:
                callback(reponse)

        return self.requete(P.REQ_LOGIN, {"username": utilisateur, "password": mot_de_passe,
                                          "ip": self.ip}, recevoir)

    def deconnexion(self, callback=None):
        def recevoir(reponse):
            self._session_terminee()
            if callback:
                callback(reponse)
        return self.requete(P.REQ_LOGOUT, {}, recevoir, delai=3.0)

    def _session_terminee(self):
        self._arreter_battement()
        self.jeton = None

    # ------------------------------------------------------------------
    # Battement de cœur : Timer réarmé périodiquement
    # ------------------------------------------------------------------
    def _armer_battement(self):
        self._arreter_battement()
        self._battement = threading.Timer(P.PERIODE_PING, self._battre)
        self._battement.daemon = True
        self._battement.start()

    def _arreter_battement(self):
        if self._battement:
            self._battement.cancel()
            self._battement = None

    def _battre(self):
        if not self.jeton:
            return

        def recevoir(reponse):
            # Si le serveur ne reconnaît plus notre session (redémarrage,
            # expiration), on prévient l'application comme pour un événement.
            if not reponse.get("ok") and "session" in reponse.get("error", ""):
                self._session_terminee()
                self.on_evenement(P.evenement(P.EVT_SESSION_CLOSED,
                                              {"reason": reponse["error"]}))
            elif reponse.get("ok") and "unread" in reponse.get("data", {}):
                self.on_evenement(P.evenement("unread", reponse["data"]))

        self.requete(P.REQ_PING, {}, recevoir)
        self._armer_battement()

    # ------------------------------------------------------------------
    # Raccourcis pour les requêtes courantes
    # ------------------------------------------------------------------
    def envoyer_mail(self, a, cc, objet, corps, pieces_jointes=(), thread_id=None,
                     in_reply_to=None, callback=None):
        return self.requete(P.REQ_SEND_MAIL, {
            "to": a, "cc": cc, "subject": objet, "body": corps,
            "attachments": list(pieces_jointes), "thread_id": thread_id,
            "in_reply_to": in_reply_to}, callback, delai=30.0)

    def lister(self, dossier, callback=None):
        return self.requete(P.REQ_LIST_MAILS, {"folder": dossier}, callback)

    def lire(self, id_mail, callback=None):
        return self.requete(P.REQ_GET_MAIL, {"mail_id": id_mail}, callback)

    def piece_jointe(self, id_mail, index, callback=None):
        return self.requete(P.REQ_GET_ATTACHMENT, {"mail_id": id_mail, "index": index},
                            callback, delai=30.0)
