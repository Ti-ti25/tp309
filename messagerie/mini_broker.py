#!/usr/bin/env python3
"""
Mini-broker MQTT 3.1.1 en Python pur (sockets TCP + threads).

Solution de secours quand Mosquitto n'est pas disponible sur la machine
(pas installé, pas de droits administrateur) :

    python3 broker.py              # écoute sur le port 1883
    python3 broker.py --port 1884 -v

Le serveur de messagerie peut aussi le lancer lui-même dans un thread
(voir lanceur_broker.py) quand aucun broker n'est disponible.

Il implémente ce dont la messagerie a besoin :
  * CONNECT / CONNACK, PINGREQ, DISCONNECT, keep-alive ;
  * PUBLISH en QoS 0 et 1 (QoS 2 accepté en réception) ;
  * SUBSCRIBE / UNSUBSCRIBE avec les jokers '+' et '#' ;
  * messages retenus (retained) ;
  * testament (Last Will) publié en cas de déconnexion anormale ;
  * sessions persistantes (clean_session = False) : abonnements conservés et
    messages QoS 1 mis en file pendant l'absence du client.

Pas d'authentification ni d'ACL : pour cela, utiliser Mosquitto.
"""

import argparse
import socket
import struct
import threading
import time

TAILLE_MAX = 32 * 1024 * 1024        # taille maximale d'un paquet (octets)
FILE_MAX = 1000                      # messages conservés pour un client absent

# Types de paquets MQTT (4 bits de poids fort du premier octet)
CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP = 1, 2, 3, 4, 5, 6, 7
SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT = (
    8, 9, 10, 11, 12, 13, 14)

bavard = False


def log(*morceaux):
    if bavard:
        print(time.strftime("%H:%M:%S"), *morceaux, flush=True)


# ----------------------------------------------------------------------
# Encodage / décodage
# ----------------------------------------------------------------------
def encoder_longueur(n):
    """Longueur restante : entier variable sur 1 à 4 octets (7 bits par octet)."""
    octets = bytearray()
    while True:
        octet, n = n % 128, n // 128
        octets.append(octet | 0x80 if n else octet)
        if not n:
            return bytes(octets)


def chaine(texte):
    donnees = texte.encode("utf-8") if isinstance(texte, str) else texte
    return struct.pack("!H", len(donnees)) + donnees


def paquet(type_paquet, drapeaux, corps):
    return bytes([(type_paquet << 4) | drapeaux]) + encoder_longueur(len(corps)) + corps


class Lecteur:
    """Lecture séquentielle des champs d'un paquet."""

    def __init__(self, donnees):
        self.donnees, self.pos = donnees, 0

    def octet(self):
        self.pos += 1
        return self.donnees[self.pos - 1]

    def entier16(self):
        self.pos += 2
        return struct.unpack("!H", self.donnees[self.pos - 2:self.pos])[0]

    def binaire(self):
        n = self.entier16()
        self.pos += n
        return self.donnees[self.pos - n:self.pos]

    def texte(self):
        return self.binaire().decode("utf-8")

    def reste(self):
        return self.donnees[self.pos:]

    def fini(self):
        return self.pos >= len(self.donnees)


def fermer_ecoute(sock):
    """
    Ferme un socket d'écoute. shutdown() débloque l'accept() en cours dans un
    autre thread (close() seul ne suffit pas sous Linux : le port resterait pris).
    """
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


def correspond(filtre, topic):
    """Le topic correspond-il au filtre d'abonnement (jokers + et #) ?"""
    f, t = filtre.split("/"), topic.split("/")
    for i, niveau in enumerate(f):
        if niveau == "#":
            return True
        if i >= len(t) or (niveau != "+" and niveau != t[i]):
            return False
    return len(f) == len(t)


# ----------------------------------------------------------------------
# Broker
# ----------------------------------------------------------------------
class Session:
    """État d'un client, éventuellement conservé après sa déconnexion."""

    def __init__(self, client_id, propre):
        self.client_id = client_id
        self.propre = propre               # clean_session
        self.abonnements = {}              # filtre -> QoS
        self.file = []                     # messages en attente (client absent)
        self.connexion = None


class Broker:
    def __init__(self):
        self.verrou = threading.RLock()
        self.sessions = {}                 # client_id -> Session
        self.retenus = {}                  # topic -> (payload, qos)

    def publier(self, topic, payload, qos, retenu):
        """Distribue un message à tous les abonnés concernés."""
        with self.verrou:
            if retenu:
                if payload:
                    self.retenus[topic] = (payload, qos)
                else:
                    self.retenus.pop(topic, None)
            destinataires = []
            for session in self.sessions.values():
                qos_max = max((q for f, q in session.abonnements.items()
                               if correspond(f, topic)), default=None)
                if qos_max is None:
                    continue
                qos_effectif = min(qos, qos_max)
                if session.connexion is not None:
                    destinataires.append((session.connexion, qos_effectif))
                elif qos_effectif > 0 and len(session.file) < FILE_MAX:
                    session.file.append((topic, payload, qos_effectif))
        for connexion, qos_effectif in destinataires:
            connexion.envoyer_publish(topic, payload, qos_effectif, False)

    def ecouter(self, hote, port):
        """Ouvre le socket d'écoute (OSError si le port est déjà pris)."""
        self.ecoute = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ecoute.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ecoute.bind((hote, port))
        self.ecoute.listen(64)

    def boucle_accueil(self):
        """Accepte les clients, un thread par connexion, jusqu'à arreter()."""
        while True:
            try:
                sock, adresse = self.ecoute.accept()
            except OSError:
                break                                  # socket d'écoute fermé
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=Connexion(self, sock, adresse).boucle, daemon=True).start()

    def demarrer_en_arriere_plan(self, hote, port):
        """Utilisé par le serveur de messagerie : le broker tourne dans un thread."""
        self.ecouter(hote, port)
        threading.Thread(target=self.boucle_accueil, name="mini-broker", daemon=True).start()

    def arreter(self):
        """Ferme le socket d'écoute et toutes les connexions clientes."""
        if getattr(self, "ecoute", None):
            fermer_ecoute(self.ecoute)
        with self.verrou:
            connexions = [s.connexion for s in self.sessions.values() if s.connexion]
        for connexion in connexions:
            connexion.fermer_socket()

    def servir(self, hote, port):
        self.ecouter(hote, port)
        print(f"Mini-broker MQTT à l'écoute sur {hote or '0.0.0.0'}:{port} (Ctrl+C pour arrêter)",
              flush=True)
        self.boucle_accueil()


class Connexion:
    """Une connexion TCP cliente, traitée dans son propre thread."""

    def __init__(self, broker, sock, adresse):
        self.broker, self.sock, self.adresse = broker, sock, adresse
        self.verrou_envoi = threading.Lock()
        self.session = None
        self.testament = None              # (topic, payload, qos, retenu)
        self.prochain_id = 0
        self.ferme = False

    # --- envoi -----------------------------------------------------------
    def envoyer(self, donnees):
        try:
            with self.verrou_envoi:
                self.sock.sendall(donnees)
        except OSError:
            self.fermer_socket()

    def envoyer_publish(self, topic, payload, qos, retenu):
        corps = chaine(topic)
        if qos:
            self.prochain_id = self.prochain_id % 65535 + 1
            corps += struct.pack("!H", self.prochain_id)
        self.envoyer(paquet(PUBLISH, (qos << 1) | (1 if retenu else 0), corps + payload))

    def fermer_socket(self):
        if not self.ferme:
            self.ferme = True
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()

    # --- réception -------------------------------------------------------
    def recevoir_exactement(self, n):
        morceaux, recu = [], 0
        while recu < n:
            morceau = self.sock.recv(min(n - recu, 1 << 20))
            if not morceau:
                raise ConnectionError("connexion fermée")
            morceaux.append(morceau)
            recu += len(morceau)
        return b"".join(morceaux)

    def lire_paquet(self):
        entete = self.recevoir_exactement(1)[0]
        longueur, multiplicateur = 0, 1
        for _ in range(4):
            octet = self.recevoir_exactement(1)[0]
            longueur += (octet & 0x7F) * multiplicateur
            multiplicateur *= 128
            if not octet & 0x80:
                break
        if longueur > TAILLE_MAX:
            raise ValueError("paquet trop volumineux")
        return entete >> 4, entete & 0x0F, self.recevoir_exactement(longueur)

    def boucle(self):
        propre = False                     # déconnexion volontaire (DISCONNECT) ?
        try:
            self.sock.settimeout(10)       # le CONNECT doit arriver vite
            type_paquet, _, corps = self.lire_paquet()
            if type_paquet != CONNECT:
                raise ValueError("le premier paquet doit être CONNECT")
            self.connecter(Lecteur(corps))
            while True:
                type_paquet, drapeaux, corps = self.lire_paquet()
                if type_paquet == DISCONNECT:
                    propre = True
                    break
                self.traiter(type_paquet, drapeaux, Lecteur(corps))
        except (OSError, ConnectionError, ValueError, IndexError, struct.error) as exc:
            log(f"{self.adresse[0]} : fin de connexion ({exc})")
        finally:
            self.deconnecter(propre)

    def connecter(self, lecteur):
        lecteur.texte()                    # nom du protocole ("MQTT" ou "MQIsdp")
        niveau = lecteur.octet()
        drapeaux = lecteur.octet()
        keepalive = lecteur.entier16()
        if niveau not in (3, 4):
            self.envoyer(paquet(CONNACK, 0, b"\x00\x01"))   # version refusée
            raise ValueError(f"version MQTT {niveau} non supportée")
        client_id = lecteur.texte()
        propre = bool(drapeaux & 0x02)
        if not client_id:
            client_id = f"auto-{id(self):x}"
        if drapeaux & 0x04:                # testament
            topic = lecteur.texte()
            message = lecteur.binaire()
            self.testament = (topic, message, (drapeaux >> 3) & 0x03, bool(drapeaux & 0x20))
        if drapeaux & 0x80:
            lecteur.texte()                # identifiant (non vérifié)
        if drapeaux & 0x40:
            lecteur.binaire()              # mot de passe (non vérifié)
        # keep-alive : sans paquet pendant 1,5 x keepalive, le client est considéré perdu
        self.sock.settimeout(keepalive * 1.5 if keepalive else None)

        with self.broker.verrou:
            ancienne = self.broker.sessions.get(client_id)
            if ancienne and ancienne.connexion is not None:
                log(f"{client_id} : reprise de session, ancienne connexion fermée")
                ancienne.connexion.fermer_socket()
                ancienne.connexion = None
            if propre or ancienne is None:
                session = Session(client_id, propre)
                presente = 0
            else:
                session, presente = ancienne, 1
            session.propre = propre
            session.connexion = self
            self.broker.sessions[client_id] = session
            self.session = session
            en_attente, session.file = session.file, []
        self.envoyer(paquet(CONNACK, 0, bytes([presente, 0])))
        log(f"{client_id} connecté depuis {self.adresse[0]} (clean_session={propre})")
        for topic, payload, qos in en_attente:
            self.envoyer_publish(topic, payload, qos, False)

    def traiter(self, type_paquet, drapeaux, lecteur):
        if type_paquet == PUBLISH:
            qos, retenu = (drapeaux >> 1) & 0x03, bool(drapeaux & 0x01)
            topic = lecteur.texte()
            if "+" in topic or "#" in topic:
                raise ValueError("joker interdit dans un topic de publication")
            id_paquet = lecteur.entier16() if qos else None
            payload = lecteur.reste()
            if qos == 1:
                self.envoyer(paquet(PUBACK, 0, struct.pack("!H", id_paquet)))
            elif qos == 2:
                self.envoyer(paquet(PUBREC, 0, struct.pack("!H", id_paquet)))
            self.broker.publier(topic, payload, min(qos, 1), retenu)
        elif type_paquet == PUBREL:
            self.envoyer(paquet(PUBCOMP, 0, lecteur.reste()[:2]))
        elif type_paquet in (PUBACK, PUBREC, PUBCOMP):
            if type_paquet == PUBREC:
                self.envoyer(paquet(PUBREL, 2, lecteur.reste()[:2]))
        elif type_paquet == SUBSCRIBE:
            id_paquet = lecteur.entier16()
            accordes, filtres = [], []
            while not lecteur.fini():
                filtre, qos = lecteur.texte(), lecteur.octet() & 0x03
                accordes.append(min(qos, 1))
                filtres.append(filtre)
                with self.broker.verrou:
                    self.session.abonnements[filtre] = min(qos, 1)
            self.envoyer(paquet(SUBACK, 0, struct.pack("!H", id_paquet) + bytes(accordes)))
            # Envoi immédiat des messages retenus correspondant aux nouveaux abonnements
            with self.broker.verrou:
                retenus = [(t, p, q) for t, (p, q) in self.broker.retenus.items()
                           if any(correspond(f, t) for f in filtres)]
            for topic, payload, qos in retenus:
                self.envoyer_publish(topic, payload, min(qos, 1), True)
        elif type_paquet == UNSUBSCRIBE:
            id_paquet = lecteur.entier16()
            with self.broker.verrou:
                while not lecteur.fini():
                    self.session.abonnements.pop(lecteur.texte(), None)
            self.envoyer(paquet(UNSUBACK, 0, struct.pack("!H", id_paquet)))
        elif type_paquet == PINGREQ:
            self.envoyer(paquet(PINGRESP, 0, b""))

    def deconnecter(self, propre):
        with self.broker.verrou:
            session = self.session
            # (si un autre client a repris cet identifiant, la session est à lui)
            if session is not None and session.connexion is self:
                session.connexion = None
                if session.propre:
                    self.broker.sessions.pop(session.client_id, None)
        self.fermer_socket()
        if session is not None:
            log(f"{session.client_id} déconnecté" + ("" if propre else " (anormalement)"))
        # Testament : publié si la connexion se termine sans DISCONNECT
        if self.testament and not propre:
            topic, payload, qos, retenu = self.testament
            self.broker.publier(topic, payload, min(qos, 1), retenu)


def main():
    global bavard
    parser = argparse.ArgumentParser(description="Mini-broker MQTT 3.1.1 (secours Mosquitto)")
    parser.add_argument("--hote", default="", help="adresse d'écoute (défaut : toutes)")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("-v", "--verbeux", action="store_true", help="journal détaillé")
    args = parser.parse_args()
    bavard = args.verbeux
    try:
        Broker().servir(args.hote, args.port)
    except OSError as exc:
        print(f"Impossible d'écouter sur le port {args.port} : {exc}")
        print("(un broker tourne peut-être déjà ; sinon essayez --port 1884)")
    except KeyboardInterrupt:
        print("\nBroker arrêté.")


if __name__ == "__main__":
    main()
