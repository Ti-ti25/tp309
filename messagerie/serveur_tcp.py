"""
Partie serveur du protocole commun R309 v1.4 (TCP brut, port 5000).

Programmation réseau "à la main" avec le module socket :
  * un socket d'écoute (bind / listen / accept) dans un thread "accueil" ;
  * un thread par client connecté, qui lit le flux TCP et le découpe en
    lignes JSON (une ligne = un message) ;
  * les commandes ne sont PAS exécutées dans ces threads : elles sont
    confiées au thread de traitement du serveur (ServeurMessagerie.executer),
    le même que pour les requêtes MQTT. Les comptes, boîtes aux lettres et
    contacts sont donc partagés : un client TCP et un client MQTT peuvent
    s'écrire.

Correspondance avec le stockage du serveur :
  * chaque participant a sa propre copie d'un mail ; l'identifiant "id" du
    protocole est le message_id commun à toutes les copies ;
  * "inbox" = toutes les copies de l'utilisateur hors corbeille ;
  * "del_mail" supprime définitivement les copies de l'utilisateur (les
    autres participants gardent les leurs, comme le demande le protocole).
"""

import socket
import threading

from . import protocole as P
from . import protocole_tcp as T
from .serveur_core import ErreurRequete, Session
from .stockage import ErreurStockage


class ErreurCommande(Exception):
    """Erreur renvoyée au client sous la forme {"type": "error", "msg": ...}."""


class ConnexionTCP:
    """Une connexion TCP cliente (lecture dans son propre thread)."""

    def __init__(self, serveur_tcp, sock, adresse):
        self.serveur_tcp = serveur_tcp
        self.sock, self.ip, self.port_client = sock, adresse[0], adresse[1]
        self.client_id = f"tcp-{self.ip}:{self.port_client}"
        self.session = None              # Session du serveur après login
        self.essais = 0                  # tentatives de login échouées
        self.verrou_envoi = threading.Lock()
        self.ferme = False

    # ------------------------------------------------------------------
    def envoyer(self, message):
        """Envoie un message (une ligne JSON). Appelable depuis n'importe quel thread."""
        if self.ferme:
            return
        try:
            with self.verrou_envoi:
                self.sock.sendall(T.encoder_ligne(message))
        except OSError:
            self.fermer()

    def erreur(self, texte):
        self.envoyer({"type": T.T_ERROR, "msg": texte})

    def fermer(self):
        if self.ferme:
            return
        self.ferme = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()

    # ------------------------------------------------------------------
    def boucle(self):
        """Thread de lecture : flux TCP -> lignes JSON -> commandes."""
        serveur = self.serveur_tcp.serveur
        decoupeur = T.DecoupeurLignes()
        # 5 minutes maximum d'inactivité avant le login
        self.sock.settimeout(T.DELAI_AVANT_LOGIN)
        try:
            while not self.ferme:
                try:
                    morceau = self.sock.recv(65536)
                except socket.timeout:
                    if self.session is None:
                        self.erreur("délai de connexion dépassé")
                        break
                    continue
                if not morceau:
                    break                        # le client a fermé la connexion
                for message in decoupeur.ajouter(morceau):
                    # exécution dans le thread de traitement du serveur
                    serveur.executer(lambda m=message: self.serveur_tcp.traiter(self, m))
        except OSError:
            pass
        finally:
            self.fermer()
            serveur.executer(lambda: self.serveur_tcp.deconnecte(self))


class ServeurTCP:
    """Écoute TCP et traitement des commandes du protocole v1.4."""

    def __init__(self, serveur, port=T.PORT_DEFAUT, hote=""):
        self.serveur = serveur            # ServeurMessagerie
        self.stockage = serveur.stockage
        self.domaine = serveur.domaine
        self.port, self.hote = int(port), hote
        self.ecoute = None
        self.connexions = set()
        self.commandes = {
            T.CMD_SEND: self._cmd_send,
            T.CMD_INBOX: self._cmd_inbox,
            T.CMD_CONTACTS: self._cmd_contacts,
            T.CMD_ADD_CONTACT: self._cmd_add_contact,
            T.CMD_DEL_CONTACT: self._cmd_del_contact,
            T.CMD_DEL_MAIL: self._cmd_del_mail,
        }

    # ------------------------------------------------------------------
    # Socket d'écoute
    # ------------------------------------------------------------------
    def demarrer(self):
        self.ecoute = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ecoute.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ecoute.bind((self.hote, self.port))       # OSError si le port est pris
        self.ecoute.listen(32)
        threading.Thread(target=self._accueil, name="accueil-tcp", daemon=True).start()

    def _accueil(self):
        while True:
            try:
                sock, adresse = self.ecoute.accept()
            except OSError:
                break                                   # socket d'écoute fermé (arrêt)
            connexion = ConnexionTCP(self, sock, adresse)
            self.connexions.add(connexion)
            self.serveur.log(f"connexion TCP depuis {adresse[0]}:{adresse[1]}")
            threading.Thread(target=connexion.boucle, name=connexion.client_id,
                             daemon=True).start()

    def arreter(self):
        if self.ecoute:
            # shutdown() débloque l'accept() du thread d'accueil (close() seul ne
            # suffit pas sous Linux : le port resterait occupé)
            try:
                self.ecoute.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.ecoute.close()
        for connexion in list(self.connexions):
            connexion.fermer()

    def deconnecte(self, connexion):
        """Fin d'une connexion (exécuté dans le thread de traitement)."""
        self.connexions.discard(connexion)
        if connexion.session is not None:
            self.serveur.fermer_session(connexion.client_id, "connexion TCP fermée")

    # ------------------------------------------------------------------
    # Conversions identifiant <-> adresse, mail interne -> mail v1.4
    # ------------------------------------------------------------------
    def identifiant(self, addr):
        """'bob@rt.iut' -> 'bob' pour le domaine local ; adresse complète sinon."""
        utilisateur, _, domaine = str(addr).partition("@")
        return utilisateur if domaine in ("", self.domaine) else str(addr)

    def mail_v14(self, mail):
        """Objet mail du protocole : {"id","from","to","subject","body","date"} (+ champs en plus)."""
        return {
            "id": mail.get("message_id") or mail["id"],
            "from": self.identifiant(mail["from"]),
            "to": [self.identifiant(a) for a in mail["to"]],
            "subject": mail["subject"], "body": mail["body"],
            "date": T.date_texte(mail["date"]),
            # champs supplémentaires, ignorés par les autres clients (règle 6)
            "cc": [self.identifiant(a) for a in mail.get("cc", [])],
            "from_name": mail.get("from_name", ""),
        }

    def contact_v14(self, utilisateur):
        """Objet contact : {"user","nom","prenom","alias","email","online","ip"}."""
        profil = self.stockage.profil(utilisateur) or {"nom": "", "prenom": "", "alias": "",
                                                       "email": ""}
        presence = self.serveur.presence(utilisateur)
        return {"user": utilisateur, "nom": profil["nom"], "prenom": profil["prenom"],
                "alias": profil["alias"], "email": profil["email"],
                "online": presence["online"],
                "ip": presence["ip"] if presence["online"] else None}

    # ------------------------------------------------------------------
    # Traitement d'une commande (thread de traitement du serveur)
    # ------------------------------------------------------------------
    def traiter(self, connexion, message):
        if connexion.ferme:
            return
        self.serveur.stats["requetes"] += 1
        cmd = message.get("cmd")
        try:
            if cmd in (T.CMD_LOGIN, T.CMD_REGISTER):
                if connexion.session is not None:
                    raise ErreurCommande("déjà authentifié")
                if cmd == T.CMD_LOGIN:
                    self._cmd_login(connexion, message)
                else:
                    self._cmd_register(connexion, message)
            elif cmd not in self.commandes:
                raise ErreurCommande(f"commande inconnue : {cmd}")
            elif connexion.session is None:
                raise ErreurCommande("authentification requise (login)")
            else:
                self.commandes[cmd](connexion, message)
        except (ErreurCommande, ErreurRequete, ErreurStockage, ValueError, TypeError,
                KeyError) as exc:
            connexion.erreur(str(exc) or "requête invalide")

    @staticmethod
    def _texte(message, champ, obligatoire=True):
        valeur = message.get(champ)
        if valeur is None and not obligatoire:
            return ""
        if not isinstance(valeur, str) or (obligatoire and not valeur.strip()):
            raise ErreurCommande(f"champ « {champ} » manquant ou invalide")
        return valeur

    def _ouvrir_session(self, connexion, utilisateur):
        session = Session(connexion.client_id, utilisateur, connexion.ip, canal="tcp",
                          connexion=connexion)
        connexion.session = session
        with self.serveur.verrou_sessions:
            self.serveur.sessions[connexion.client_id] = session
        self.stockage.noter_connexion(utilisateur, connexion.ip)
        self.serveur.publier_presence(utilisateur)
        self.serveur.log(f"{utilisateur} connecté en TCP depuis {connexion.ip}")
        self.serveur._signaler("sessions")
        connexion.sock.settimeout(None)                   # plus de délai après le login
        connexion.envoyer({"type": T.T_LOGIN_OK})

    def _cmd_login(self, connexion, message):
        utilisateur = self._texte(message, "user").strip().lower()
        mot_de_passe = self._texte(message, "password", obligatoire=False)
        if self.stockage.verifier_mot_de_passe(utilisateur, mot_de_passe):
            self._ouvrir_session(connexion, utilisateur)
            return
        connexion.essais += 1
        self.serveur.log(f"échec d'authentification TCP pour « {utilisateur} » "
                         f"({connexion.essais}/{T.ESSAIS_MAX})", "warning")
        connexion.erreur("identifiants invalides")
        if connexion.essais >= T.ESSAIS_MAX:
            connexion.fermer()                            # 5 essais maximum

    def _cmd_register(self, connexion, message):
        utilisateur = self._texte(message, "user").strip()
        if not T.RE_IDENTIFIANT.match(utilisateur):
            raise ErreurCommande("identifiant invalide ([A-Za-z0-9_.-], 32 caractères max)")
        utilisateur = utilisateur.lower()
        nom = self._texte(message, "nom").strip()
        prenom = self._texte(message, "prenom").strip()
        self.stockage.creer_utilisateur(utilisateur, self._texte(message, "password"), nom,
                                        prenom, self._texte(message, "alias", False),
                                        self._texte(message, "email", False))
        self.serveur.log(f"nouveau compte (TCP) : {utilisateur}")
        self._ouvrir_session(connexion, utilisateur)      # session ouverte directement

    def _cmd_send(self, connexion, message):
        destinataires = message.get("to")
        if isinstance(destinataires, str):
            destinataires = [destinataires]
        if not isinstance(destinataires, list) or not destinataires or \
                not all(isinstance(d, str) for d in destinataires):
            raise ErreurCommande("champ « to » : liste d'identifiants attendue")
        adresses = P.liste_adresses(destinataires, self.domaine)
        # Vérification AVANT l'envoi : un destinataire local inconnu = erreur
        for addr in adresses:
            utilisateur, domaine = P.decouper_adresse(addr)
            if domaine == self.domaine and not (self.stockage.utilisateur_existe(utilisateur)
                                                or self.stockage.groupe(utilisateur)):
                raise ErreurCommande(f"destinataire inconnu : {utilisateur}")
        # Envoi par la partie "SMTP" du serveur ; elle pousse le mail à
        # l'expéditeur et aux destinataires connectés (pousser_tcp).
        self.serveur.sortant.envoyer(connexion.session, {
            "to": adresses, "cc": [],
            "subject": self._texte(message, "subject", obligatoire=False),
            "body": self._texte(message, "body", obligatoire=False)})

    def _cmd_inbox(self, connexion, message):
        vus, mails = set(), []
        copies = sorted(self.stockage.mails(connexion.session.utilisateur),
                        key=lambda m: m["date"])
        for mail in copies:
            if mail["folder"] == P.DOSSIER_CORBEILLE:
                continue
            identifiant = mail.get("message_id") or mail["id"]
            if identifiant not in vus:                    # 1 seule fois (envoi à soi-même)
                vus.add(identifiant)
                mails.append(self.mail_v14(mail))
        connexion.envoyer({"type": T.T_INBOX, "mails": mails})

    def _envoyer_contacts(self, connexion):
        contacts = []
        for contact in self.stockage.contacts(connexion.session.utilisateur):
            utilisateur, domaine = P.decouper_adresse(contact["address"])
            if domaine == self.domaine and self.stockage.utilisateur_existe(utilisateur):
                contacts.append(self.contact_v14(utilisateur))
        connexion.envoyer({"type": T.T_CONTACTS, "contacts": contacts})

    def _cmd_contacts(self, connexion, message):
        self._envoyer_contacts(connexion)

    def _cmd_add_contact(self, connexion, message):
        utilisateur = self._texte(message, "contact").strip().lower()
        moi = connexion.session.utilisateur
        if utilisateur == moi:
            raise ErreurCommande("impossible de s'ajouter soi-même")
        profil = self.stockage.profil(utilisateur)
        if profil is None:
            raise ErreurCommande(f"utilisateur inconnu : {utilisateur}")
        self.stockage.enregistrer_contact(moi, {
            "address": P.adresse(utilisateur, self.domaine), "nom": profil["nom"],
            "prenom": profil["prenom"], "alias": "", "notes": ""})
        self._envoyer_contacts(connexion)

    def _cmd_del_contact(self, connexion, message):
        utilisateur = self._texte(message, "contact").strip().lower()
        self.stockage.supprimer_contact(connexion.session.utilisateur,
                                        P.adresse(utilisateur, self.domaine))
        self._envoyer_contacts(connexion)

    def _cmd_del_mail(self, connexion, message):
        identifiant = message.get("id")
        moi = connexion.session.utilisateur
        copies = [m for m in self.stockage.mails(moi)
                  if identifiant is not None and identifiant in (m.get("message_id"), m["id"])]
        if not copies:
            raise ErreurCommande("mail inconnu")
        for copie in copies:                              # suppression pour CET utilisateur
            self.stockage.supprimer_mail(moi, copie["id"])
        connexion.envoyer({"type": T.T_MAIL_DELETED, "id": identifiant})
