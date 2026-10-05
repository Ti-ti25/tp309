"""
Serveur de messagerie (sans interface graphique).

Le serveur est un client MQTT particulier : il est abonné aux topics de
requêtes de son domaine et répond à chaque client sur un topic qui lui est
propre. Le broker Mosquitto ne fait que transporter les messages ; toute
l'intelligence (comptes, routage, boîtes aux lettres) est ici.

Conformément au point 7 du sujet, le serveur est découpé en deux composants :

  * ServeurSortant  ("SMTP") : reçoit les mails à envoyer, les route vers les
    boîtes locales, développe les listes de diffusion, relaie vers les autres
    domaines et génère les avis de non-distribution ("bounces") ;
  * ServeurEntrant  ("IMAP") : gère la consultation des boîtes aux lettres
    (lister, lire, marquer lu, déplacer, supprimer, rechercher).

La classe ServeurMessagerie les assemble : connexion au broker, gestion des
sessions, présence, annuaire, contacts et groupes.

Modèle d'exécution (multithreading) :

  * thread réseau paho (loop_start)  : reçoit les messages MQTT et appelle
    on_message. Ce callback ne fait que DÉPOSER la requête dans une file ;
  * thread "traitement"              : consomme la file et exécute les requêtes
    une par une (pas de blocage du réseau pendant un hachage de mot de passe,
    ordre des requêtes préservé) ;
  * Timer périodique "expiration"    : ferme les sessions dont le client ne
    donne plus signe de vie (battement de cœur absent) ;
  * thread de l'IHM (éventuel)       : est prévenu des changements par le
    mécanisme d'écouteurs (pattern Observateur), voir ajouter_ecouteur().
"""

import logging
import queue
import threading

import paho.mqtt.client as mqtt

from . import protocole as P
from .stockage import ErreurStockage, Stockage

journal = logging.getLogger("messagerie.serveur")

# Tolérance (s) sur l'âge d'une requête : les requêtes plus anciennes (mises
# en file d'attente par le broker pendant un arrêt du serveur) sont ignorées,
# le client ayant de toute façon déjà abandonné.
AGE_MAX_REQUETE = 30.0


class ErreurRequete(Exception):
    """Erreur renvoyée telle quelle au client dans une réponse négative."""


class Session:
    """Une session = un client MQTT authentifié (un même utilisateur peut en avoir plusieurs)."""

    def __init__(self, client_id, utilisateur, ip):
        self.client_id = client_id
        self.utilisateur = utilisateur
        self.jeton = P.nouvel_id()
        self.ip = ip
        self.debut = P.maintenant()
        self.derniere_activite = self.debut


# ======================================================================
# Partie "SMTP" : envoi, routage, relais
# ======================================================================
class ServeurSortant:
    """Routage des mails sortants (local, listes de diffusion, autres domaines)."""

    def __init__(self, serveur):
        self.serveur = serveur
        self.stockage = serveur.stockage
        self.domaine = serveur.domaine

    # ------------------------------------------------------------------
    def envoyer(self, session, donnees):
        """Traite une requête send_mail émise par un utilisateur local."""
        expediteur = session.utilisateur
        a = P.liste_adresses(donnees.get("to", []), self.domaine)
        cc = P.liste_adresses(donnees.get("cc", []), self.domaine)
        if not a and not cc:
            raise ErreurRequete("aucun destinataire")
        objet = str(donnees.get("subject", "")).strip()[:300] or "(sans objet)"
        corps = str(donnees.get("body", ""))
        if len(corps) > 1_000_000:
            raise ErreurRequete("message trop long")

        profil = self.stockage.profil(expediteur)
        mail = {
            "from": P.adresse(expediteur, self.domaine),
            "from_name": f"{profil['prenom']} {profil['nom']}".strip(),
            "to": a, "cc": cc,
            "subject": objet, "body": corps, "date": P.maintenant(),
            "message_id": P.nouvel_id(),
            "thread_id": donnees.get("thread_id") or P.nouvel_id(),
            "in_reply_to": donnees.get("in_reply_to"),
            "attachments": self._preparer_pj(expediteur, donnees.get("attachments", [])),
            "via": None,
        }

        # Copie dans les "Éléments envoyés" de l'expéditeur.
        copie = self.stockage.deposer(expediteur, mail, P.DOSSIER_ENVOYES, lu=True)
        self.serveur.stats["mails_envoyes"] += 1
        self.serveur.log(f"{mail['from']} envoie « {objet} » à {', '.join(a + cc)}")

        livres, relayes, echecs = self.router(mail, a + cc)
        return {"mail_id": copie["id"], "delivered": livres,
                "relayed": relayes, "failed": echecs}

    # ------------------------------------------------------------------
    def _preparer_pj(self, expediteur, pieces):
        """
        Les pièces jointes arrivent soit avec leur contenu (base64, nouvelle
        pièce), soit sous forme de référence SHA-256 (transfert d'un mail déjà
        reçu : inutile de renvoyer le fichier). Dans ce second cas on vérifie
        que l'utilisateur possède bien un mail contenant cette pièce.
        """
        resultat = []
        for pj in pieces or []:
            nom = str(pj.get("name") or "piece_jointe").replace("/", "_")[:200]
            mime = str(pj.get("mime") or "application/octet-stream")
            if "data" in pj:
                octets = P.decoder_pj(pj["data"])
                if len(octets) > P.TAILLE_MAX_PJ:
                    raise ErreurRequete(f"pièce jointe trop volumineuse : {nom}")
                empreinte = self.stockage.stocker_pj(octets)
                taille = len(octets)
            elif "sha256" in pj and self.stockage.possede_pj(expediteur, pj["sha256"]):
                empreinte, taille = pj["sha256"], int(pj.get("size", 0))
            else:
                raise ErreurRequete(f"pièce jointe invalide : {nom}")
            resultat.append({"name": nom, "mime": mime, "size": taille, "sha256": empreinte})
        return resultat

    # ------------------------------------------------------------------
    def router(self, mail, destinataires):
        """
        Distribue un mail à une liste d'adresses. Les adresses locales sont
        livrées immédiatement ; les adresses d'autres domaines sont regroupées
        par domaine et relayées au serveur concerné.
        Renvoie (livrés, relayés, échecs).
        """
        livres, relayes, echecs = [], [], []
        deja_servis = set()          # évite les doublons (destinataire direct + membre d'une liste)
        distants = {}
        for addr in destinataires:
            utilisateur, domaine = P.decouper_adresse(addr)
            if domaine == self.domaine:
                try:
                    self.livrer_local(utilisateur, mail, deja_servis)
                    livres.append(addr)
                except ErreurRequete as exc:
                    echecs.append({"address": addr, "reason": str(exc)})
            else:
                distants.setdefault(domaine, []).append(addr)
        for domaine, adresses in distants.items():
            self.relayer(domaine, adresses, mail)
            relayes.extend(adresses)
        return livres, relayes, echecs

    def livrer_local(self, nom, mail, deja_servis):
        """Livre à un utilisateur local, ou à tous les membres d'une liste locale."""
        if self.stockage.utilisateur_existe(nom):
            if nom not in deja_servis:
                deja_servis.add(nom)
                self._deposer_et_notifier(nom, mail)
            return
        groupe = self.stockage.groupe(nom)
        if groupe is None:
            raise ErreurRequete("destinataire inconnu")
        adresse_groupe = P.adresse(nom, self.domaine)
        mail_liste = dict(mail, via=adresse_groupe)
        self.serveur.log(f"développement de la liste {adresse_groupe} "
                         f"({len(groupe['members'])} membre(s))")
        for membre in groupe["members"]:
            # L'expéditeur, s'il est membre, a déjà sa copie dans "envoyés".
            if P.adresse(membre, self.domaine) != mail["from"] and membre not in deja_servis:
                deja_servis.add(membre)
                self._deposer_et_notifier(membre, mail_liste)

    def _deposer_et_notifier(self, nom, mail):
        copie = self.stockage.deposer(nom, mail, P.DOSSIER_RECEPTION)
        self.serveur.stats["mails_livres"] += 1
        # Notification "push" de toutes les sessions ouvertes du destinataire.
        self.serveur.notifier(nom, P.evenement(P.EVT_NEW_MAIL, {
            "mail": ServeurEntrant.entete(copie),
            "unread": self.stockage.nb_non_lus(nom)}))

    # ------------------------------------------------------------------
    # Relais inter-domaines
    # ------------------------------------------------------------------
    def relayer(self, domaine, adresses, mail):
        """Transmet un mail au serveur d'un autre domaine (pièces jointes incluses)."""
        mail_relais = dict(mail)
        mail_relais["attachments"] = [
            dict(pj, data=P.encoder_pj(self.stockage.lire_pj(pj["sha256"])))
            for pj in mail.get("attachments", [])]
        message = {"v": P.VERSION, "type": "relay", "from_domain": self.domaine,
                   "rcpt": adresses, "mail": mail_relais}
        self.serveur.publier(P.topic_relais(domaine), message)
        self.serveur.stats["mails_relayes"] += 1
        self.serveur.log(f"relais vers le domaine {domaine} : {', '.join(adresses)}")

    def recevoir_relais(self, message):
        """Un autre serveur nous transmet un mail destiné à nos utilisateurs."""
        mail = dict(message["mail"])
        pieces = []
        for pj in mail.get("attachments", []):
            octets = P.decoder_pj(pj.pop("data", ""))
            pieces.append(dict(pj, sha256=self.stockage.stocker_pj(octets), size=len(octets)))
        mail["attachments"] = pieces
        self.serveur.stats["relais_recus"] += 1
        self.serveur.log(f"mail relayé reçu de {message.get('from_domain')} "
                         f"pour {', '.join(message.get('rcpt', []))}")
        deja_servis, echecs = set(), []
        for addr in message.get("rcpt", []):
            try:
                utilisateur, domaine = P.decouper_adresse(addr)
                if domaine != self.domaine:
                    raise ErreurRequete("domaine incorrect")
                self.livrer_local(utilisateur, mail, deja_servis)
            except (ErreurRequete, ValueError) as exc:
                echecs.append({"address": addr, "reason": str(exc)})
        if echecs:
            self.rebond(mail, echecs)

    def rebond(self, mail_origine, echecs):
        """
        Avis de non-distribution envoyé à l'expéditeur par "mailer-daemon".
        On ne génère jamais de rebond pour un rebond (évite les boucles).
        """
        if mail_origine["from"].startswith(P.MAILER_DAEMON + "@"):
            return
        details = "\n".join(f"  - {e['address']} : {e['reason']}" for e in echecs)
        avis = {
            "from": P.adresse(P.MAILER_DAEMON, self.domaine),
            "from_name": "Serveur de messagerie",
            "to": [mail_origine["from"]], "cc": [],
            "subject": f"Échec de distribution : {mail_origine['subject']}",
            "body": ("Votre message n'a pas pu être remis aux destinataires suivants :\n\n"
                     f"{details}\n\n----- Message d'origine -----\n"
                     f"Objet : {mail_origine['subject']}\n"
                     f"Date  : {P.date_lisible(mail_origine['date'])}\n\n"
                     f"{mail_origine['body']}"),
            "date": P.maintenant(), "message_id": P.nouvel_id(),
            "thread_id": mail_origine.get("thread_id"),
            "in_reply_to": mail_origine.get("message_id"),
            "attachments": [], "via": None,
        }
        self.serveur.stats["rebonds"] += 1
        self.serveur.log(f"avis de non-distribution pour {mail_origine['from']}", "warning")
        self.router(avis, [mail_origine["from"]])


# ======================================================================
# Partie "IMAP" : consultation des boîtes aux lettres
# ======================================================================
class ServeurEntrant:
    """Opérations sur la boîte aux lettres de l'utilisateur connecté."""

    def __init__(self, serveur):
        self.stockage = serveur.stockage

    @staticmethod
    def entete(mail):
        """Version allégée d'un mail pour l'affichage en liste (sans le corps)."""
        return {
            "id": mail["id"], "folder": mail["folder"], "read": mail["read"],
            "from": mail["from"], "from_name": mail.get("from_name", ""),
            "to": mail["to"], "cc": mail["cc"], "subject": mail["subject"],
            "date": mail["date"], "thread_id": mail.get("thread_id"),
            "via": mail.get("via"), "nb_attachments": len(mail.get("attachments", [])),
            "preview": mail["body"][:120].replace("\n", " "),
        }

    def lister(self, session, donnees):
        dossier = donnees.get("folder", P.DOSSIER_RECEPTION)
        if dossier not in P.DOSSIERS:
            raise ErreurRequete("dossier inconnu")
        mails = self.stockage.mails(session.utilisateur, dossier)
        mails.sort(key=lambda m: m["date"], reverse=True)
        return {"folder": dossier, "mails": [self.entete(m) for m in mails],
                "unread": self.stockage.nb_non_lus(session.utilisateur)}

    def lire(self, session, donnees):
        mail = self.stockage.mail(session.utilisateur, donnees.get("mail_id"))
        if donnees.get("mark_read", True) and not mail["read"]:
            self.stockage.modifier_mail(session.utilisateur, mail["id"], read=True)
        return {"mail": dict(mail), "unread": self.stockage.nb_non_lus(session.utilisateur)}

    def piece_jointe(self, session, donnees):
        mail = self.stockage.mail(session.utilisateur, donnees.get("mail_id"))
        try:
            pj = mail["attachments"][int(donnees.get("index", -1))]
        except (IndexError, ValueError):
            raise ErreurRequete("pièce jointe inconnue") from None
        return dict(pj, data=P.encoder_pj(self.stockage.lire_pj(pj["sha256"])))

    def marquer(self, session, donnees):
        self.stockage.modifier_mail(session.utilisateur, donnees.get("mail_id"),
                                    read=bool(donnees.get("read", True)))
        return {"unread": self.stockage.nb_non_lus(session.utilisateur)}

    def deplacer(self, session, donnees):
        dossier = donnees.get("folder")
        if dossier not in P.DOSSIERS:
            raise ErreurRequete("dossier inconnu")
        self.stockage.modifier_mail(session.utilisateur, donnees.get("mail_id"), folder=dossier)
        return {"unread": self.stockage.nb_non_lus(session.utilisateur)}

    def supprimer(self, session, donnees):
        """Premier appel : vers la corbeille. Depuis la corbeille : suppression définitive."""
        mail = self.stockage.mail(session.utilisateur, donnees.get("mail_id"))
        if mail["folder"] == P.DOSSIER_CORBEILLE:
            self.stockage.supprimer_mail(session.utilisateur, mail["id"])
            definitif = True
        else:
            self.stockage.modifier_mail(session.utilisateur, mail["id"],
                                        folder=P.DOSSIER_CORBEILLE)
            definitif = False
        return {"deleted": definitif, "unread": self.stockage.nb_non_lus(session.utilisateur)}

    def rechercher(self, session, donnees):
        """
        Renvoie les mails complets (hors corbeille) d'un fil de discussion
        (thread_id) ou échangés avec un correspondant (correspondent), triés
        chronologiquement : sert à l'export d'une discussion.
        """
        fil = donnees.get("thread_id")
        correspondant = donnees.get("correspondent")
        if correspondant:
            correspondant = P.normaliser_adresse(correspondant)
        resultat = []
        for mail in self.stockage.mails(session.utilisateur):
            if mail["folder"] == P.DOSSIER_CORBEILLE:
                continue
            if fil and mail.get("thread_id") != fil:
                continue
            if correspondant:
                if mail["folder"] == P.DOSSIER_ENVOYES:
                    concerne = correspondant in mail["to"] + mail["cc"]
                else:
                    concerne = mail["from"] == correspondant or mail.get("via") == correspondant
                if not concerne:
                    continue
            resultat.append(mail)
        resultat.sort(key=lambda m: m["date"])
        return {"mails": resultat}


# ======================================================================
# Serveur complet
# ======================================================================
class ServeurMessagerie:
    """Serveur de messagerie d'un domaine, connecté à un broker MQTT."""

    def __init__(self, domaine="rt.iut", hote="localhost", port=1883,
                 identifiant=None, mot_de_passe=None, repertoire="donnees"):
        if not P.domaine_valide(domaine):
            raise ValueError(f"nom de domaine invalide : {domaine!r}")
        self.domaine = domaine
        self.hote, self.port = hote, int(port)
        self.identifiant, self.mot_de_passe = identifiant, mot_de_passe
        self.stockage = Stockage(f"{repertoire}/{domaine}")
        self.sortant = ServeurSortant(self)
        self.entrant = ServeurEntrant(self)

        self.sessions = {}                     # client_id -> Session
        self.verrou_sessions = threading.RLock()
        self.ecouteurs = []                    # callbacks de l'IHM (pattern Observateur)
        self.stats = {"requetes": 0, "mails_envoyes": 0, "mails_livres": 0,
                      "mails_relayes": 0, "relais_recus": 0, "rebonds": 0, "erreurs": 0}
        self.demarre_le = None
        self.connecte = False

        self._file = queue.Queue()             # file des messages à traiter
        self._thread_traitement = None
        self._minuterie = None
        self._actif = False
        self.mqtt = None

        # Table de dispatch : type de requête -> méthode de traitement.
        self._traitements = {
            P.REQ_REGISTER: self._req_inscription,
            P.REQ_LOGIN: self._req_connexion,
            P.REQ_LOGOUT: self._req_deconnexion,
            P.REQ_PING: self._req_ping,
            P.REQ_UPDATE_PROFILE: self._req_profil,
            P.REQ_SEND_MAIL: self.sortant.envoyer,
            P.REQ_LIST_MAILS: self.entrant.lister,
            P.REQ_GET_MAIL: self.entrant.lire,
            P.REQ_GET_ATTACHMENT: self.entrant.piece_jointe,
            P.REQ_MARK_READ: self.entrant.marquer,
            P.REQ_MOVE_MAIL: self.entrant.deplacer,
            P.REQ_DELETE_MAIL: self.entrant.supprimer,
            P.REQ_SEARCH_MAILS: self.entrant.rechercher,
            P.REQ_LIST_USERS: self._req_annuaire,
            P.REQ_GET_CONTACTS: self._req_contacts,
            P.REQ_SAVE_CONTACT: self._req_enregistrer_contact,
            P.REQ_DELETE_CONTACT: self._req_supprimer_contact,
            P.REQ_LIST_GROUPS: self._req_groupes,
            P.REQ_CREATE_GROUP: self._req_creer_groupe,
            P.REQ_JOIN_GROUP: self._req_abonner,
            P.REQ_LEAVE_GROUP: self._req_desabonner,
            P.REQ_DELETE_GROUP: self._req_supprimer_groupe,
        }

    # ------------------------------------------------------------------
    # Observateurs (utilisés par l'IHM du serveur)
    # ------------------------------------------------------------------
    def ajouter_ecouteur(self, callback):
        """callback(type_evenement, donnees) sera appelé depuis un thread quelconque."""
        self.ecouteurs.append(callback)

    def _signaler(self, type_evt, **donnees):
        for callback in list(self.ecouteurs):
            try:
                callback(type_evt, donnees)
            except Exception:  # un écouteur défaillant ne doit pas tuer le serveur
                journal.exception("écouteur en erreur")

    def log(self, message, niveau="info"):
        getattr(journal, niveau)(message)
        self._signaler("log", niveau=niveau, message=message)

    # ------------------------------------------------------------------
    # Démarrage / arrêt
    # ------------------------------------------------------------------
    def demarrer(self):
        """Connexion au broker et lancement des threads. Lève OSError si le broker est injoignable."""
        self._actif = True
        self.demarre_le = P.maintenant()
        # clean_session=False : le broker conserve nos abonnements et met en
        # file d'attente les messages QoS 1 (relais inter-domaines notamment)
        # reçus pendant une coupure du serveur.
        self.mqtt = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                                client_id=f"srv-{self.domaine}", clean_session=False)
        if self.identifiant:
            self.mqtt.username_pw_set(self.identifiant, self.mot_de_passe)
        # Testament : si le serveur disparaît brutalement, le broker publiera
        # lui-même ce message à sa place -> les clients sont prévenus.
        self.mqtt.will_set(P.topic_statut_serveur(self.domaine),
                           P.encoder({"v": P.VERSION, "online": False, "domain": self.domaine}),
                           qos=1, retain=True)
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_disconnect = self._on_disconnect
        self.mqtt.on_message = self._on_message
        self.mqtt.reconnect_delay_set(1, 10)

        self._thread_traitement = threading.Thread(target=self._boucle_traitement,
                                                   name="traitement", daemon=True)
        self._thread_traitement.start()
        self.log(f"connexion au broker {self.hote}:{self.port}...")
        try:
            self.mqtt.connect(self.hote, self.port, keepalive=30)
        except OSError:
            self._actif = False
            self._file.put(None)
            raise
        self.mqtt.loop_start()               # lance le thread réseau de paho
        self._armer_minuterie()

    def arreter(self):
        """Arrêt propre : les utilisateurs passent hors ligne, le statut aussi."""
        if not self._actif:
            return
        self._actif = False
        if self._minuterie:
            self._minuterie.cancel()
        with self.verrou_sessions:
            utilisateurs = {s.utilisateur for s in self.sessions.values()}
            for session in list(self.sessions.values()):
                self.publier(P.topic_evenement(self.domaine, session.client_id),
                             P.evenement(P.EVT_SESSION_CLOSED, {"reason": "arrêt du serveur"}))
            self.sessions.clear()
        for utilisateur in utilisateurs:
            self.publier_presence(utilisateur)
        info = self.publier(P.topic_statut_serveur(self.domaine),
                            {"v": P.VERSION, "online": False, "domain": self.domaine},
                            retenu=True)
        if info is not None and self.connecte:
            info.wait_for_publish(2)
        self._file.put(None)                 # réveille et arrête le thread de traitement
        self.mqtt.disconnect()
        self.mqtt.loop_stop()
        self.connecte = False
        self.log("serveur arrêté")
        self._signaler("etat", connecte=False)
        self._signaler("sessions")

    # ------------------------------------------------------------------
    # Callbacks MQTT (thread réseau paho)
    # ------------------------------------------------------------------
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.log(f"connexion refusée par le broker : {reason_code}", "error")
            return
        self.connecte = True
        client.subscribe([(P.topic_requetes_serveur(self.domaine), 1),
                          (P.topic_deconnexions_serveur(self.domaine), 1),
                          (P.topic_relais(self.domaine), 1)])
        self.publier(P.topic_statut_serveur(self.domaine),
                     {"v": P.VERSION, "online": True, "domain": self.domaine,
                      "since": self.demarre_le}, retenu=True)
        # Les présences "retenues" d'une exécution précédente peuvent être
        # fausses (arrêt brutal) : on republie l'état réel de chacun.
        for profil in self.stockage.liste_utilisateurs():
            self.publier_presence(profil["username"])
        self.log(f"serveur du domaine « {self.domaine} » en ligne", "info")
        self._signaler("etat", connecte=True)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connecte = False
        if self._actif:
            self.log(f"connexion au broker perdue ({reason_code}), reconnexion...", "warning")
        self._signaler("etat", connecte=False)

    def _on_message(self, client, userdata, msg):
        """Ne fait que mettre le message en file : le traitement a lieu ailleurs."""
        self._file.put((msg.topic, msg.payload))

    # ------------------------------------------------------------------
    # Thread de traitement
    # ------------------------------------------------------------------
    def _boucle_traitement(self):
        while True:
            element = self._file.get()
            if element is None:
                break
            topic, contenu = element
            try:
                self._traiter(topic, contenu)
            except Exception as exc:
                self.stats["erreurs"] += 1
                journal.exception("erreur de traitement")
                self.log(f"erreur interne : {exc}", "error")

    def _traiter(self, topic, contenu):
        segments = P.analyser_topic(topic)
        # mail/<domaine>/server/<request|disconnect|relay>[/<client_id>]
        if len(segments) < 4 or segments[1] != self.domaine or segments[2] != "server":
            return
        nature = segments[3]
        if nature == "disconnect" and len(segments) == 5:
            self.fermer_session(segments[4], "connexion perdue (testament MQTT)")
            return
        try:
            message = P.decoder(contenu)
        except ValueError as exc:
            self.log(f"message rejeté sur {topic} : {exc}", "warning")
            return
        if nature == "relay":
            self.sortant.recevoir_relais(message)
        elif nature == "request" and len(segments) == 5:
            self._traiter_requete(segments[4], message)

    def _traiter_requete(self, client_id, req):
        """
        Exécute une requête et publie la réponse sur le topic du client.
        Le client_id est extrait du topic : avec les ACL Mosquitto fournies,
        un client ne peut publier que sur SON topic de requête, il ne peut donc
        pas se faire passer pour un autre.
        """
        self.stats["requetes"] += 1
        if P.maintenant() - float(req.get("ts", P.maintenant())) > AGE_MAX_REQUETE:
            self.log(f"requête périmée ignorée ({req.get('type')})", "warning")
            return
        type_req = req.get("type")
        traitement = self._traitements.get(type_req)
        try:
            if traitement is None:
                raise ErreurRequete(f"requête inconnue : {type_req}")
            session = self._session_de(client_id, req)
            if session is None and type_req not in P.REQUETES_ANONYMES:
                raise ErreurRequete("session invalide ou expirée, reconnectez-vous")
            if type_req in P.REQUETES_ANONYMES:
                donnees = traitement(client_id, session, req.get("data") or {})
            else:
                donnees = traitement(session, req.get("data") or {})
            reponse = P.reponse_ok(req, donnees)
        except (ErreurRequete, ErreurStockage, ValueError) as exc:
            reponse = P.reponse_erreur(req, str(exc))
        self.publier(P.topic_reponse(self.domaine, client_id), reponse)

    # ------------------------------------------------------------------
    # Outils
    # ------------------------------------------------------------------
    def publier(self, topic, message, retenu=False):
        if self.mqtt is None:
            return None
        return self.mqtt.publish(topic, P.encoder(message), qos=1, retain=retenu)

    def notifier(self, utilisateur, evt):
        """Pousse un événement vers toutes les sessions ouvertes d'un utilisateur."""
        with self.verrou_sessions:
            cibles = [s.client_id for s in self.sessions.values() if s.utilisateur == utilisateur]
        for client_id in cibles:
            self.publier(P.topic_evenement(self.domaine, client_id), evt)

    def notifier_tous(self, evt):
        with self.verrou_sessions:
            cibles = list(self.sessions)
        for client_id in cibles:
            self.publier(P.topic_evenement(self.domaine, client_id), evt)

    def sessions_de(self, utilisateur):
        with self.verrou_sessions:
            return [s for s in self.sessions.values() if s.utilisateur == utilisateur]

    def presence(self, utilisateur):
        """État de présence d'un utilisateur, tel que publié sur MQTT."""
        profil = self.stockage.profil(utilisateur) or {}
        sessions = self.sessions_de(utilisateur)
        return {
            "v": P.VERSION, "address": P.adresse(utilisateur, self.domaine),
            "nom": profil.get("nom", ""), "prenom": profil.get("prenom", ""),
            "online": bool(sessions),
            "ip": sessions[-1].ip if sessions else profil.get("derniere_ip"),
            "since": min(s.debut for s in sessions) if sessions else None,
            "last_seen": (max(s.derniere_activite for s in sessions) if sessions
                          else profil.get("derniere_connexion")),
            "sessions": len(sessions),
        }

    def publier_presence(self, utilisateur):
        """Présence en message "retenu" : un client qui s'abonne plus tard la reçoit aussitôt."""
        self.publier(P.topic_presence(self.domaine, utilisateur), self.presence(utilisateur),
                     retenu=True)

    def _session_de(self, client_id, req):
        with self.verrou_sessions:
            session = self.sessions.get(client_id)
            if session is None or session.jeton != req.get("token"):
                return None
            session.derniere_activite = P.maintenant()
            return session

    def fermer_session(self, client_id, raison):
        with self.verrou_sessions:
            session = self.sessions.pop(client_id, None)
        if session is None:
            return
        self.stockage.noter_connexion(session.utilisateur, session.ip)
        self.publier_presence(session.utilisateur)
        self.log(f"{session.utilisateur} déconnecté ({raison})")
        self._signaler("sessions")

    def expulser(self, client_id):
        """Fermeture d'une session à l'initiative de l'administrateur (IHM)."""
        self.publier(P.topic_evenement(self.domaine, client_id),
                     P.evenement(P.EVT_SESSION_CLOSED,
                                 {"reason": "session fermée par l'administrateur"}))
        self.fermer_session(client_id, "expulsé par l'administrateur")

    def liste_sessions(self):
        with self.verrou_sessions:
            return list(self.sessions.values())

    # ------------------------------------------------------------------
    # Minuterie d'expiration des sessions (threading.Timer réarmé)
    # ------------------------------------------------------------------
    def _armer_minuterie(self):
        if not self._actif:
            return
        self._minuterie = threading.Timer(P.PERIODE_PING / 2, self._verifier_sessions)
        self._minuterie.daemon = True
        self._minuterie.start()

    def _verifier_sessions(self):
        limite = P.maintenant() - P.EXPIRATION_SESSION
        with self.verrou_sessions:
            expirees = [cid for cid, s in self.sessions.items() if s.derniere_activite < limite]
        for client_id in expirees:
            self.fermer_session(client_id, "session expirée")
        self._signaler("stats")
        self._armer_minuterie()

    # ==================================================================
    # Traitement des requêtes de session / annuaire / contacts / groupes
    # ==================================================================
    def _req_inscription(self, client_id, session, donnees):
        utilisateur = str(donnees.get("username", "")).strip().lower()
        self.stockage.creer_utilisateur(utilisateur, donnees.get("password", ""),
                                        donnees.get("nom", ""), donnees.get("prenom", ""))
        self.log(f"nouveau compte : {utilisateur}")
        self.publier_presence(utilisateur)
        self._signaler("sessions")
        return {"address": P.adresse(utilisateur, self.domaine)}

    def _req_connexion(self, client_id, session, donnees):
        utilisateur = str(donnees.get("username", "")).strip().lower()
        if not self.stockage.verifier_mot_de_passe(utilisateur, donnees.get("password", "")):
            self.log(f"échec d'authentification pour « {utilisateur} »", "warning")
            raise ErreurRequete("identifiant ou mot de passe incorrect")
        nouvelle = Session(client_id, utilisateur, str(donnees.get("ip") or "?"))
        with self.verrou_sessions:
            ancienne = self.sessions.get(client_id)
            self.sessions[client_id] = nouvelle
        if ancienne and ancienne.utilisateur != utilisateur:
            self.publier_presence(ancienne.utilisateur)
        self.stockage.noter_connexion(utilisateur, nouvelle.ip)
        self.publier_presence(utilisateur)
        self.log(f"{utilisateur} connecté depuis {nouvelle.ip}")
        self._signaler("sessions")
        return {"token": nouvelle.jeton, "address": P.adresse(utilisateur, self.domaine),
                "profile": self.stockage.profil(utilisateur),
                "unread": self.stockage.nb_non_lus(utilisateur)}

    def _req_deconnexion(self, session, donnees):
        self.fermer_session(session.client_id, "déconnexion volontaire")
        return {}

    def _req_ping(self, client_id, session, donnees):
        reponse = {"time": P.maintenant(), "domain": self.domaine, "authenticated": bool(session)}
        if session:
            reponse["unread"] = self.stockage.nb_non_lus(session.utilisateur)
        return reponse

    def _req_profil(self, session, donnees):
        self.stockage.modifier_profil(session.utilisateur, donnees.get("nom"),
                                      donnees.get("prenom"), donnees.get("password"))
        self.publier_presence(session.utilisateur)
        self._signaler("sessions")
        return {"profile": self.stockage.profil(session.utilisateur)}

    # --- annuaire et contacts -----------------------------------------
    def _req_annuaire(self, session, donnees):
        return {"users": [self.presence(p["username"])
                          for p in self.stockage.liste_utilisateurs()]}

    def _req_contacts(self, session, donnees):
        contacts = []
        for contact in self.stockage.contacts(session.utilisateur):
            contact = dict(contact)
            utilisateur, domaine = P.decouper_adresse(contact["address"])
            if domaine == self.domaine and self.stockage.utilisateur_existe(utilisateur):
                etat = self.presence(utilisateur)
                contact.update(online=etat["online"], ip=etat["ip"], last_seen=etat["last_seen"])
            contacts.append(contact)
        return {"contacts": contacts}

    def _req_enregistrer_contact(self, session, donnees):
        addr = P.normaliser_adresse(donnees.get("address", ""), self.domaine)
        contact = {"address": addr,
                   "nom": str(donnees.get("nom", "")).strip(),
                   "prenom": str(donnees.get("prenom", "")).strip(),
                   "alias": str(donnees.get("alias", "")).strip(),
                   "notes": str(donnees.get("notes", "")).strip()}
        # Pré-remplissage à partir de l'annuaire pour un utilisateur local.
        utilisateur, domaine = P.decouper_adresse(addr)
        profil = self.stockage.profil(utilisateur) if domaine == self.domaine else None
        if profil:
            contact["nom"] = contact["nom"] or profil["nom"]
            contact["prenom"] = contact["prenom"] or profil["prenom"]
        self.stockage.enregistrer_contact(session.utilisateur, contact)
        return {"contact": contact}

    def _req_supprimer_contact(self, session, donnees):
        self.stockage.supprimer_contact(session.utilisateur,
                                        P.normaliser_adresse(donnees.get("address", ""),
                                                             self.domaine))
        return {}

    # --- listes de diffusion -------------------------------------------
    def _description_groupe(self, nom, groupe, utilisateur):
        return {"name": nom, "address": P.adresse(nom, self.domaine),
                "description": groupe["description"],
                "owner": P.adresse(groupe["owner"], self.domaine),
                "members": [P.adresse(m, self.domaine) for m in groupe["members"]],
                "member": utilisateur in groupe["members"]}

    def _req_groupes(self, session, donnees):
        return {"groups": [self._description_groupe(nom, g, session.utilisateur)
                           for nom, g in self.stockage.groupes().items()]}

    def _groupes_modifies(self, message):
        self.log(message)
        self.notifier_tous(P.evenement(P.EVT_GROUPS_CHANGED))
        self._signaler("groupes")

    def _nom_groupe(self, donnees):
        nom = str(donnees.get("name", "")).strip().lower()
        return nom.split("@")[0]

    def _req_creer_groupe(self, session, donnees):
        nom = self._nom_groupe(donnees)
        self.stockage.creer_groupe(nom, session.utilisateur, donnees.get("description", ""))
        self._groupes_modifies(f"{session.utilisateur} crée la liste {nom}")
        return {"address": P.adresse(nom, self.domaine)}

    def _req_abonner(self, session, donnees):
        nom = self._nom_groupe(donnees)
        self.stockage.abonner(nom, session.utilisateur)
        self._groupes_modifies(f"{session.utilisateur} s'abonne à {nom}")
        return {}

    def _req_desabonner(self, session, donnees):
        nom = self._nom_groupe(donnees)
        self.stockage.desabonner(nom, session.utilisateur)
        self._groupes_modifies(f"{session.utilisateur} se désabonne de {nom}")
        return {}

    def _req_supprimer_groupe(self, session, donnees):
        nom = self._nom_groupe(donnees)
        self.stockage.supprimer_groupe(nom, session.utilisateur)
        self._groupes_modifies(f"{session.utilisateur} supprime la liste {nom}")
        return {}


def main():
    """Lancement du serveur en mode console (sans IHM)."""
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Serveur de messagerie MQTT (mode console)")
    parser.add_argument("--domaine", default="rt.iut")
    parser.add_argument("--hote", default="localhost", help="adresse du broker Mosquitto")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--utilisateur", help="identifiant MQTT du serveur (si ACL)")
    parser.add_argument("--mot-de-passe", help="mot de passe MQTT du serveur (si ACL)")
    parser.add_argument("--donnees", default="donnees", help="répertoire de stockage")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    serveur = ServeurMessagerie(args.domaine, args.hote, args.port, args.utilisateur,
                                args.mot_de_passe, args.donnees)
    fin = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: fin.set())
    signal.signal(signal.SIGTERM, lambda *_: fin.set())
    serveur.demarrer()
    while not fin.wait(0.5):     # attente interruptible par Ctrl+C
        pass
    serveur.arreter()


if __name__ == "__main__":
    main()
