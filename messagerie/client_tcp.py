"""
Client du protocole commun R309 v1.4 (TCP brut, une ligne JSON par message).

ClientTCP est un ADAPTATEUR : il offre exactement la même interface que
ClientMessagerie (client MQTT) -- requete(type, données, callback),
connexion(), on_evenement, on_presence... -- ce qui permet à l'interface
graphique de fonctionner sans modification avec l'un ou l'autre protocole.

Différences à combler :
  * le protocole v1.4 n'a pas d'identifiant de requête : les réponses sont
    associées aux commandes grâce à une FILE D'ATTENTE (FIFO) des commandes
    envoyées et au type de la réponse attendue ("inbox", "contacts",
    "login_ok"...). Un "error" correspond à la plus ancienne commande en attente ;
  * le serveur ne connaît pas de dossiers : "reception" = mails dont je suis
    destinataire, "envoyes" = mails dont je suis l'expéditeur ; l'état lu /
    non lu est géré localement ;
  * la présence des contacts n'est pas poussée par le serveur : un
    threading.Timer redemande la liste des contacts toutes les 10 s ;
  * pièces jointes, Cc, listes de diffusion, profil : non prévus par le
    protocole, donc refusés proprement (voir capacites).

Threads : un thread lit le socket en continu ; les callbacks sont appelés
depuis ce thread (ou depuis un Timer) -> l'IHM doit les retransmettre à
Tkinter (PontThreads), exactement comme avec le client MQTT.
"""

import json
import os
import socket
import threading
import time

from . import protocole as P
from . import protocole_tcp as T

PERIODE_CONTACTS = 10.0                 # rafraîchissement de la présence (s)
FICHIER_LUS = os.path.join(os.path.expanduser("~"), ".messagerie_lus_tcp.json")


def sonder_serveur_tcp(hote, port, delai=3.0):
    """Teste qu'un serveur TCP écoute sur hote:port. Renvoie (ok, message, latence_ms)."""
    debut = time.perf_counter()
    try:
        with socket.create_connection((hote, int(port)), timeout=delai):
            latence = (time.perf_counter() - debut) * 1000
            return True, "serveur TCP joignable", latence
    except OSError as exc:
        return False, f"serveur injoignable : {exc}", None


def _entete(mail):
    """En-tête d'un mail pour l'affichage en liste (même format que le serveur MQTT)."""
    return {"id": mail["id"], "folder": mail["folder"], "read": mail["read"],
            "from": mail["from"], "from_name": mail.get("from_name", ""),
            "to": mail["to"], "cc": mail["cc"], "subject": mail["subject"],
            "date": mail["date"], "thread_id": mail.get("thread_id"), "via": None,
            "nb_attachments": 0, "preview": mail["body"][:120].replace("\n", " ")}


class ClientTCP:
    """Connexion d'un utilisateur à un serveur parlant le protocole TCP v1.4."""

    protocole = "tcp"
    capacites = {"pj": False, "groupes": False, "profil": False, "corbeille": False,
                 "cc": False}

    def __init__(self, hote="localhost", port=T.PORT_DEFAUT, domaine="rt.iut"):
        self.hote, self.port, self.domaine = hote, int(port), domaine
        self.client_id = "tcp"
        self.ip = "?"
        self.jeton = None
        self.utilisateur = None
        self.adresse = None
        self.profil = {"username": "", "nom": "", "prenom": ""}
        self.serveur_en_ligne = None
        self.presences = {}                   # adresse -> présence (même format que MQTT)

        self.mails = {}                       # id (texte) -> mail au format v1.4
        self.contacts = []                    # contacts au format v1.4
        self.lus = set()                      # ids des mails déjà lus (local)
        self._casse = {}                      # identifiant en minuscules -> casse d'origine
        self._attente = []                    # commandes en attente de réponse (FIFO)
        self._verrou = threading.RLock()
        self._verrou_envoi = threading.Lock()
        self._sock = None
        self._ferme = False
        self._minuterie = None

        # --- callbacks à redéfinir par l'application (cf. ClientMessagerie) ---
        self.on_evenement = lambda evt: None
        self.on_presence = lambda presence: None
        self.on_statut_serveur = lambda statut: None
        self.on_connexion_mqtt = lambda connecte, msg: None   # (nom gardé pour l'IHM)

    @property
    def connecte(self):
        return self.jeton is not None

    # ------------------------------------------------------------------
    # Socket
    # ------------------------------------------------------------------
    def connecter(self, delai=5.0):
        """Ouvre la connexion TCP (OSError si le serveur est injoignable)."""
        self._sock = socket.create_connection((self.hote, self.port), timeout=delai)
        self._sock.settimeout(None)
        self.ip, port_local = self._sock.getsockname()[:2]
        self.client_id = f"tcp-{self.ip}:{port_local}"
        self.serveur_en_ligne = True
        threading.Thread(target=self._lire, name="lecture-tcp", daemon=True).start()

    def fermer(self):
        """Fermeture volontaire (aucun callback d'erreur)."""
        self._ferme = True
        self.jeton = None
        if self._minuterie:
            self._minuterie.cancel()
        with self._verrou:
            for entree in self._attente:
                entree["minuterie"].cancel()
            self._attente.clear()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def _envoyer(self, message):
        with self._verrou_envoi:
            self._sock.sendall(T.encoder_ligne(message))

    def _lire(self):
        """Thread de lecture : flux TCP -> lignes JSON -> _recevoir()."""
        decoupeur = T.DecoupeurLignes()
        try:
            while True:
                morceau = self._sock.recv(65536)
                if not morceau:
                    break
                for message in decoupeur.ajouter(morceau):
                    try:
                        self._recevoir(message)
                    except Exception as exc:      # un message bizarre ne tue pas la lecture
                        print("message TCP ignoré :", repr(exc))
        except OSError:
            pass
        self._connexion_perdue()

    def _connexion_perdue(self):
        """Équivalent du message local "offline" du protocole."""
        if self._ferme:
            return
        self._ferme = True
        self.serveur_en_ligne = False
        with self._verrou:
            attentes, self._attente = self._attente, []
        for entree in attentes:
            entree["minuterie"].cancel()
            entree["fin"]({"type": T.T_ERROR, "msg": "connexion au serveur perdue"})
        self.on_statut_serveur({"online": False})
        if self.jeton:
            self.jeton = None
            self.on_evenement(P.evenement(P.EVT_SESSION_CLOSED,
                                          {"reason": "connexion au serveur perdue"}))

    # ------------------------------------------------------------------
    # Commandes et association des réponses (FIFO)
    # ------------------------------------------------------------------
    def _commande(self, message, attendus, traduire, callback, delai=P.DELAI_REQUETE):
        """
        Envoie une commande. Quand une réponse d'un des types `attendus` (ou
        une erreur) arrive, traduire(réponse) produit les données renvoyées au
        callback au format interne {"ok": True, "data": ...}.
        """
        def fin(reponse):
            if reponse.get("type") == T.T_ERROR:
                resultat = {"ok": False, "error": str(reponse.get("msg") or "erreur")}
            else:
                try:
                    resultat = {"ok": True, "data": traduire(reponse)}
                except Exception as exc:
                    resultat = {"ok": False, "error": f"réponse invalide : {exc}"}
            if callback:
                callback(resultat)

        entree = {"attendus": set(attendus), "fin": fin, "cmd": message.get("cmd")}
        entree["minuterie"] = threading.Timer(delai, self._expiration, args=(entree,))
        entree["minuterie"].daemon = True
        with self._verrou:
            self._attente.append(entree)
        entree["minuterie"].start()
        try:
            self._envoyer(message)
        except (OSError, AttributeError):
            self._expiration(entree, "non connecté au serveur")

    def _expiration(self, entree, texte="le serveur ne répond pas (délai dépassé)"):
        with self._verrou:
            if not any(e is entree for e in self._attente):
                return
            self._attente.remove(entree)
        entree["minuterie"].cancel()
        entree["fin"]({"type": T.T_ERROR, "msg": texte})

    def _trouver_attente(self, message):
        """Commande en attente à laquelle correspond ce message (ou None)."""
        type_msg = message.get("type")
        with self._verrou:
            if type_msg == T.T_ERROR:
                return self._attente[0] if self._attente else None
            for entree in self._attente:
                if type_msg in entree["attendus"]:
                    # un "mail" ne répond à "send" que s'il vient de moi
                    if type_msg == T.T_MAIL and not self._de_moi(message.get("mail") or {}):
                        continue
                    return entree
        return None

    def _recevoir(self, message):
        """Traite un message reçu du serveur (thread de lecture)."""
        type_msg = message.get("type")
        entree = self._trouver_attente(message)
        if entree is not None:
            with self._verrou:
                if any(e is entree for e in self._attente):
                    self._attente.remove(entree)
                else:
                    entree = None
        if entree is not None:
            entree["minuterie"].cancel()

        # 1. mise à jour du cache local
        nouveau = False
        if type_msg == T.T_MAIL and isinstance(message.get("mail"), dict):
            nouveau = self._integrer_mail(message["mail"])
        elif type_msg == T.T_INBOX and isinstance(message.get("mails"), list):
            with self._verrou:
                self.mails = {}
                for mail in message["mails"]:
                    if isinstance(mail, dict) and "id" in mail:
                        self._integrer_mail(mail)
        elif type_msg == T.T_CONTACTS and isinstance(message.get("contacts"), list):
            self._maj_contacts(message["contacts"])
        elif type_msg == T.T_MAIL_DELETED:
            with self._verrou:
                self.mails.pop(str(message.get("id")), None)

        # 2. réponse à une commande, ou événement spontané
        if entree is not None:
            entree["fin"](message)
        elif type_msg == T.T_MAIL and nouveau and not self._de_moi(message["mail"]):
            entete = _entete(self._mail_interne(message["mail"], P.DOSSIER_RECEPTION))
            self.on_evenement(P.evenement(P.EVT_NEW_MAIL, {"mail": entete,
                                                           "unread": self._non_lus()}))
        elif type_msg == T.T_ERROR:
            self.on_connexion_mqtt(False, f"Serveur : {message.get('msg')}")

    # ------------------------------------------------------------------
    # Cache local et conversions v1.4 <-> format interne
    # ------------------------------------------------------------------
    def _integrer_mail(self, mail):
        """Ajoute un mail au cache ; renvoie True s'il était inconnu (dédoublonnage par id)."""
        cle = str(mail.get("id"))
        with self._verrou:
            nouveau = cle not in self.mails
            self.mails[cle] = mail
        for ident in [mail.get("from")] + list(mail.get("to") or []):
            if isinstance(ident, str):
                self._casse[ident.lower()] = ident
        return nouveau

    def _adresse(self, ident):
        """'bob' -> 'bob@<domaine>' (les adresses complètes restent telles quelles)."""
        ident = str(ident)
        return ident.lower() if "@" in ident else f"{ident.lower()}@{self.domaine}"

    def _ident(self, addr):
        """'bob@<domaine>' -> 'bob' (avec la casse d'origine connue du serveur)."""
        utilisateur, _, domaine = addr.partition("@")
        if domaine and domaine != self.domaine:
            return addr
        return self._casse.get(utilisateur.lower(), utilisateur)

    def _de_moi(self, mail):
        return bool(self.utilisateur) and str(mail.get("from", "")).lower() == self.utilisateur

    def _pour_moi(self, mail):
        tous = list(mail.get("to") or []) + list(mail.get("cc") or [])
        return bool(self.utilisateur) and self.utilisateur in [str(x).lower() for x in tous]

    def _nom_de(self, ident):
        for contact in self.contacts:
            if str(contact.get("user", "")).lower() == str(ident).lower():
                return f"{contact.get('prenom', '')} {contact.get('nom', '')}".strip()
        return ""

    def _mail_interne(self, mail, dossier):
        cle = str(mail.get("id"))
        return {
            "id": cle, "message_id": cle, "folder": dossier,
            "read": cle in self.lus or dossier == P.DOSSIER_ENVOYES,
            "from": self._adresse(mail.get("from", "?")),
            "from_name": mail.get("from_name") or self._nom_de(mail.get("from", "")),
            "to": [self._adresse(x) for x in mail.get("to") or []],
            "cc": [self._adresse(x) for x in mail.get("cc") or []],
            "subject": str(mail.get("subject", "")), "body": str(mail.get("body", "")),
            "date": T.date_horodatage(mail.get("date")),
            "thread_id": T.fil_de_discussion(mail.get("subject", "")),
            "in_reply_to": None, "attachments": [], "via": None,
        }

    def _non_lus(self):
        with self._verrou:
            return sum(1 for cle, m in self.mails.items()
                       if self._pour_moi(m) and not self._de_moi(m) and cle not in self.lus)

    def _liste(self, dossier):
        with self._verrou:
            mails = list(self.mails.values())
        if dossier == P.DOSSIER_RECEPTION:
            choisis = [m for m in mails if self._pour_moi(m)]
        elif dossier == P.DOSSIER_ENVOYES:
            choisis = [m for m in mails if self._de_moi(m)]
        else:
            choisis = []                      # pas de corbeille : la suppression est définitive
        entetes = [_entete(self._mail_interne(m, dossier)) for m in choisis]
        entetes.sort(key=lambda m: m["date"], reverse=True)
        return {"folder": dossier, "mails": entetes, "unread": self._non_lus()}

    def _maj_contacts(self, contacts):
        """Met à jour les contacts et signale les changements de présence."""
        self.contacts = [c for c in contacts if isinstance(c, dict) and c.get("user")]
        for contact in self.contacts:
            self._casse[str(contact["user"]).lower()] = str(contact["user"])
            addr = self._adresse(contact["user"])
            en_ligne = bool(contact.get("online"))
            ancien = self.presences.get(addr)
            presence = {"v": P.VERSION, "address": addr, "nom": contact.get("nom", ""),
                        "prenom": contact.get("prenom", ""), "online": en_ligne,
                        "ip": contact.get("ip"),
                        "last_seen": time.time() if en_ligne else
                        (ancien or {}).get("last_seen"),
                        "sessions": 1 if en_ligne else 0}
            self.presences[addr] = presence
            if ancien is None or ancien["online"] != en_ligne or ancien["ip"] != presence["ip"]:
                self.on_presence(presence)

    def _contacts_internes(self):
        return [{"address": self._adresse(c["user"]), "nom": c.get("nom", ""),
                 "prenom": c.get("prenom", ""), "alias": c.get("alias", ""),
                 "notes": c.get("email", ""), "email": c.get("email", ""),
                 "online": bool(c.get("online")), "ip": c.get("ip"),
                 "last_seen": self.presences.get(self._adresse(c["user"]), {}).get("last_seen")}
                for c in self.contacts]

    # --- état lu / non lu, conservé entre deux lancements -------------
    def _cle_lus(self):
        return f"{self.hote}:{self.port}:{self.utilisateur}"

    def _charger_lus(self):
        try:
            with open(FICHIER_LUS, encoding="utf-8") as fichier:
                self.lus = set(json.load(fichier).get(self._cle_lus(), []))
        except (OSError, ValueError, AttributeError):
            self.lus = set()

    def _sauver_lus(self):
        try:
            with open(FICHIER_LUS, encoding="utf-8") as fichier:
                tout = json.load(fichier)
        except (OSError, ValueError):
            tout = {}
        tout[self._cle_lus()] = sorted(self.lus)
        try:
            with open(FICHIER_LUS, "w", encoding="utf-8") as fichier:
                json.dump(tout, fichier)
        except OSError:
            pass

    # --- rafraîchissement périodique de la présence (Timer réarmé) ----
    def _armer_minuterie(self):
        if self._ferme or not self.jeton:
            return
        self._minuterie = threading.Timer(PERIODE_CONTACTS, self._rafraichir)
        self._minuterie.daemon = True
        self._minuterie.start()

    def _rafraichir(self):
        if self.jeton:
            self._commande({"cmd": T.CMD_CONTACTS}, [T.T_CONTACTS], lambda r: None, None)
        self._armer_minuterie()

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    def _session_ouverte(self, utilisateur, nom="", prenom=""):
        self.jeton = "tcp"
        self.utilisateur = utilisateur.lower()
        self._casse[self.utilisateur] = utilisateur
        self.adresse = self._adresse(utilisateur)
        self.profil = {"username": self.utilisateur, "nom": nom, "prenom": prenom}
        self._charger_lus()
        # chargement initial : boîte et contacts, puis présence toutes les 10 s
        self._commande({"cmd": T.CMD_INBOX}, [T.T_INBOX], lambda r: None, None)
        self._commande({"cmd": T.CMD_CONTACTS}, [T.T_CONTACTS], lambda r: None, None)
        self._armer_minuterie()

    def connexion(self, utilisateur, mot_de_passe, callback=None):
        return self.requete(P.REQ_LOGIN, {"username": utilisateur, "password": mot_de_passe},
                            callback)

    def inscrire(self, utilisateur, mot_de_passe, nom, prenom, callback=None):
        return self.requete(P.REQ_REGISTER, {"username": utilisateur, "password": mot_de_passe,
                                             "nom": nom, "prenom": prenom}, callback)

    def deconnexion(self, callback=None):
        return self.requete(P.REQ_LOGOUT, {}, callback)

    def requete_sync(self, type_req, donnees=None, delai=P.DELAI_REQUETE):
        """Version bloquante de requete() (tests, scripts). NE PAS appeler depuis Tkinter."""
        fini, resultat = threading.Event(), {}

        def recevoir(reponse):
            resultat.update(reponse)
            fini.set()

        self.requete(type_req, donnees, recevoir, delai)
        fini.wait(delai + 1)
        return resultat or {"ok": False, "error": "délai dépassé"}

    # ------------------------------------------------------------------
    # Requêtes au format interne -> commandes v1.4
    # ------------------------------------------------------------------
    def requete(self, type_req, donnees=None, callback=None, delai=P.DELAI_REQUETE):
        d = donnees or {}

        def local(data=None, erreur=None):
            """Réponse calculée sur place, sans dialogue avec le serveur."""
            if callback:
                callback({"ok": False, "error": erreur} if erreur else
                         {"ok": True, "data": data or {}})

        non_prevu = "fonction non prévue par le protocole TCP v1.4"

        if type_req == P.REQ_LOGIN:
            utilisateur = str(d.get("username", "")).strip()

            def apres_login(reponse):
                self._session_ouverte(utilisateur)
                return {"token": self.jeton, "address": self.adresse, "profile": self.profil,
                        "unread": 0}
            self._commande({"cmd": T.CMD_LOGIN, "user": utilisateur,
                            "password": d.get("password", "")},
                           [T.T_LOGIN_OK], apres_login, callback, delai)

        elif type_req == P.REQ_REGISTER:
            utilisateur = str(d.get("username", "")).strip()
            message = {"cmd": T.CMD_REGISTER, "user": utilisateur,
                       "password": d.get("password", ""), "nom": d.get("nom", ""),
                       "prenom": d.get("prenom", "")}

            def apres_inscription(reponse):        # le compte est créé ET la session ouverte
                self._session_ouverte(utilisateur, d.get("nom", ""), d.get("prenom", ""))
                return {"address": self.adresse}
            self._commande(message, [T.T_LOGIN_OK], apres_inscription, callback, delai)

        elif type_req == P.REQ_LOGOUT:
            self.fermer()                          # v1.4 : on ferme simplement la connexion
            local({})

        elif type_req == P.REQ_PING:
            local({"time": time.time(), "authenticated": self.connecte,
                   "unread": self._non_lus()})

        elif type_req == P.REQ_SEND_MAIL:
            if d.get("attachments"):
                return local(erreur="pièces jointes non prévues par le protocole TCP v1.4")
            destinataires = list(d.get("to") or []) + list(d.get("cc") or [])  # pas de Cc en v1.4
            idents = [self._ident(a) for a in destinataires]
            self._commande({"cmd": T.CMD_SEND, "to": idents, "subject": d.get("subject", ""),
                            "body": d.get("body", "")}, [T.T_MAIL],
                           lambda r: {"mail_id": str(r["mail"].get("id")),
                                      "delivered": destinataires, "relayed": [], "failed": []},
                           callback, max(delai, 15.0))

        elif type_req == P.REQ_LIST_MAILS:
            dossier = d.get("folder", P.DOSSIER_RECEPTION)
            if dossier == P.DOSSIER_CORBEILLE:
                return local(self._liste(dossier))
            self._commande({"cmd": T.CMD_INBOX}, [T.T_INBOX], lambda r: self._liste(dossier),
                           callback, delai)

        elif type_req == P.REQ_GET_MAIL:
            mail = self.mails.get(str(d.get("mail_id")))
            if mail is None:
                return local(erreur="mail introuvable")
            dossier = P.DOSSIER_RECEPTION if self._pour_moi(mail) else P.DOSSIER_ENVOYES
            if d.get("mark_read", True) and str(mail["id"]) not in self.lus:
                self.lus.add(str(mail["id"]))
                self._sauver_lus()
            local({"mail": self._mail_interne(mail, dossier), "unread": self._non_lus()})

        elif type_req == P.REQ_MARK_READ:
            cle = str(d.get("mail_id"))
            (self.lus.add if d.get("read", True) else self.lus.discard)(cle)
            self._sauver_lus()
            local({"unread": self._non_lus()})

        elif type_req == P.REQ_DELETE_MAIL:
            mail = self.mails.get(str(d.get("mail_id")))
            if mail is None:
                return local(erreur="mail introuvable")
            self._commande({"cmd": T.CMD_DEL_MAIL, "id": mail["id"]}, [T.T_MAIL_DELETED],
                           lambda r: {"deleted": True, "unread": self._non_lus()},
                           callback, delai)

        elif type_req == P.REQ_SEARCH_MAILS:
            fil, correspondant = d.get("thread_id"), d.get("correspondent")
            resultat = []
            for mail in list(self.mails.values()):
                dossier = P.DOSSIER_ENVOYES if self._de_moi(mail) else P.DOSSIER_RECEPTION
                interne = self._mail_interne(mail, dossier)
                if fil and interne["thread_id"] != fil:
                    continue
                if correspondant:
                    correspondant = correspondant.lower()
                    if dossier == P.DOSSIER_ENVOYES:
                        if correspondant not in interne["to"] + interne["cc"]:
                            continue
                    elif interne["from"] != correspondant:
                        continue
                resultat.append(interne)
            resultat.sort(key=lambda m: m["date"])
            local({"mails": resultat})

        elif type_req in (P.REQ_GET_CONTACTS, P.REQ_LIST_USERS):
            cle = "contacts" if type_req == P.REQ_GET_CONTACTS else "users"
            self._commande({"cmd": T.CMD_CONTACTS}, [T.T_CONTACTS],
                           lambda r: {cle: self._contacts_internes()}, callback, delai)

        elif type_req == P.REQ_SAVE_CONTACT:
            ident = self._ident(P.normaliser_adresse(d.get("address", ""), self.domaine))
            if any(str(c.get("user", "")).lower() == ident.lower() for c in self.contacts):
                # v1.4 : les informations d'un contact sont fixées par le serveur
                return local({"contact": {"address": self._adresse(ident)}})
            self._commande({"cmd": T.CMD_ADD_CONTACT, "contact": ident}, [T.T_CONTACTS],
                           lambda r: {"contact": {"address": self._adresse(ident)}},
                           callback, delai)

        elif type_req == P.REQ_DELETE_CONTACT:
            ident = self._ident(P.normaliser_adresse(d.get("address", ""), self.domaine))
            self._commande({"cmd": T.CMD_DEL_CONTACT, "contact": ident}, [T.T_CONTACTS],
                           lambda r: {}, callback, delai)

        elif type_req == P.REQ_LIST_GROUPS:
            local({"groups": []})

        else:   # pièces jointes, dossiers, profil, listes de diffusion...
            local(erreur=non_prevu)
