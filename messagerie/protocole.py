"""
Protocole applicatif de la messagerie, transporté par MQTT.

MQTT est un protocole "publish / subscribe" : les programmes ne se parlent pas
directement, ils publient des messages sur des *topics* et s'abonnent aux topics
qui les intéressent. Le broker Mosquitto se charge de la distribution.
C'est donc un modèle intrinsèquement événementiel : chaque message reçu
déclenche l'appel d'une fonction callback (on_message).

Ce module définit, en un seul endroit, tout ce que doivent partager le serveur
et les clients pour être interopérables (cf. PROTOCOLE.md) :

  * l'arborescence des topics ;
  * le format des messages (JSON, champ "v" = version du protocole) ;
  * les types de requêtes / événements ;
  * quelques fonctions utilitaires (validation des adresses, encodage...).

Arborescence des topics (<d> = domaine du serveur, <cid> = client-id MQTT) :

    mail/<d>/server/request/<cid>     client  -> serveur  : requêtes
    mail/<d>/client/<cid>/response    serveur -> client   : réponses aux requêtes
    mail/<d>/client/<cid>/event       serveur -> client   : notifications "push"
    mail/<d>/server/disconnect/<cid>  client  -> serveur  : testament (LWT) du client
    mail/<d>/server/status            serveur -> tous     : état du serveur (retained + LWT)
    mail/<d>/presence/<user>          serveur -> tous     : présence d'un utilisateur (retained)
    mail/<d>/server/relay             serveur -> serveur  : relais inter-domaines (MTA)
"""

import base64
import json
import re
import time
import uuid

# Version du protocole : incrémentée en cas de changement incompatible.
VERSION = 1

# Racine de tous les topics de l'application.
RACINE = "mail"

# ---------------------------------------------------------------------------
# Types de requêtes (client -> serveur)
# ---------------------------------------------------------------------------
REQ_REGISTER = "register"            # création de compte
REQ_LOGIN = "login"                  # ouverture de session
REQ_LOGOUT = "logout"                # fermeture de session
REQ_PING = "ping"                    # battement de cœur (keep-alive applicatif)
REQ_UPDATE_PROFILE = "update_profile"

REQ_SEND_MAIL = "send_mail"          # partie "SMTP" : envoi d'un mail
REQ_LIST_MAILS = "list_mails"        # partie "IMAP" : liste des en-têtes d'un dossier
REQ_GET_MAIL = "get_mail"            # lecture d'un mail complet
REQ_GET_ATTACHMENT = "get_attachment"  # téléchargement d'une pièce jointe
REQ_MARK_READ = "mark_read"          # marquer lu / non lu
REQ_MOVE_MAIL = "move_mail"          # déplacer vers un dossier
REQ_DELETE_MAIL = "delete_mail"      # corbeille puis suppression définitive
REQ_SEARCH_MAILS = "search_mails"    # recherche (fil de discussion, correspondant)

REQ_LIST_USERS = "list_users"        # annuaire du domaine
REQ_GET_CONTACTS = "get_contacts"    # carnet d'adresses personnel
REQ_SAVE_CONTACT = "save_contact"
REQ_DELETE_CONTACT = "delete_contact"

REQ_LIST_GROUPS = "list_groups"      # listes de diffusion
REQ_CREATE_GROUP = "create_group"
REQ_JOIN_GROUP = "join_group"
REQ_LEAVE_GROUP = "leave_group"
REQ_DELETE_GROUP = "delete_group"

# Requêtes autorisées sans session ouverte.
REQUETES_ANONYMES = {REQ_REGISTER, REQ_LOGIN, REQ_PING}

# ---------------------------------------------------------------------------
# Événements (serveur -> client, sans requête préalable)
# ---------------------------------------------------------------------------
EVT_NEW_MAIL = "new_mail"            # un nouveau mail vient d'arriver
EVT_GROUPS_CHANGED = "groups_changed"  # une liste de diffusion a été modifiée
EVT_SESSION_CLOSED = "session_closed"  # le serveur a fermé la session

# ---------------------------------------------------------------------------
# Dossiers d'une boîte aux lettres
# ---------------------------------------------------------------------------
DOSSIER_RECEPTION = "reception"
DOSSIER_ENVOYES = "envoyes"
DOSSIER_CORBEILLE = "corbeille"
DOSSIERS = (DOSSIER_RECEPTION, DOSSIER_ENVOYES, DOSSIER_CORBEILLE)
LIBELLES_DOSSIERS = {
    DOSSIER_RECEPTION: "Boîte de réception",
    DOSSIER_ENVOYES: "Éléments envoyés",
    DOSSIER_CORBEILLE: "Corbeille",
}

# Expéditeur des messages d'erreur de distribution ("bounce").
MAILER_DAEMON = "mailer-daemon"

# Noms qu'un utilisateur ou un groupe ne peut pas prendre.
NOMS_RESERVES = {MAILER_DAEMON, "postmaster", "serveur", "server", "admin"}

# Taille maximale d'une pièce jointe (octets, avant encodage base64).
TAILLE_MAX_PJ = 10 * 1024 * 1024

# Délai (s) au bout duquel une requête sans réponse est considérée en échec.
DELAI_REQUETE = 8.0

# Période (s) du battement de cœur client et délai d'expiration côté serveur.
PERIODE_PING = 20.0
EXPIRATION_SESSION = 3 * PERIODE_PING

# Expressions régulières de validation (les noms servent aussi dans les
# topics MQTT : ils ne doivent donc contenir ni '/', ni '+', ni '#').
_RE_NOM = re.compile(r"^[a-z0-9][a-z0-9._-]{1,31}$")
_RE_DOMAINE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,62}$")


# ---------------------------------------------------------------------------
# Construction des topics
# ---------------------------------------------------------------------------
def topic_requete(domaine, client_id):
    """Topic sur lequel un client publie ses requêtes."""
    return f"{RACINE}/{domaine}/server/request/{client_id}"


def topic_requetes_serveur(domaine):
    """Filtre d'abonnement du serveur pour recevoir toutes les requêtes."""
    return f"{RACINE}/{domaine}/server/request/+"


def topic_reponse(domaine, client_id):
    """Topic sur lequel le serveur répond à un client donné."""
    return f"{RACINE}/{domaine}/client/{client_id}/response"


def topic_evenement(domaine, client_id):
    """Topic sur lequel le serveur pousse des notifications à un client."""
    return f"{RACINE}/{domaine}/client/{client_id}/event"


def topic_client(domaine, client_id):
    """Filtre d'abonnement d'un client (réponses + événements)."""
    return f"{RACINE}/{domaine}/client/{client_id}/#"


def topic_deconnexion(domaine, client_id):
    """Topic du testament (Last Will) d'un client."""
    return f"{RACINE}/{domaine}/server/disconnect/{client_id}"


def topic_deconnexions_serveur(domaine):
    return f"{RACINE}/{domaine}/server/disconnect/+"


def topic_statut_serveur(domaine):
    """État (en ligne / hors ligne) du serveur d'un domaine."""
    return f"{RACINE}/{domaine}/server/status"


def topic_presence(domaine, utilisateur):
    """Présence (retained) d'un utilisateur."""
    return f"{RACINE}/{domaine}/presence/{utilisateur}"


def topic_presences_tous_domaines():
    """Filtre pour suivre la présence de tous les utilisateurs de tous les domaines."""
    return f"{RACINE}/+/presence/+"


def topic_relais(domaine):
    """Topic de relais inter-serveurs (équivalent du SMTP entre MTA)."""
    return f"{RACINE}/{domaine}/server/relay"


def analyser_topic(topic):
    """Découpe un topic en liste de segments ; renvoie [] si hors application."""
    segments = topic.split("/")
    if len(segments) < 3 or segments[0] != RACINE:
        return []
    return segments


# ---------------------------------------------------------------------------
# Construction / décodage des messages JSON
# ---------------------------------------------------------------------------
def nouvel_id():
    """Identifiant unique (requêtes, mails, fils de discussion...)."""
    return uuid.uuid4().hex


def maintenant():
    """Horodatage courant (secondes depuis l'epoch, float)."""
    return time.time()


def requete(type_req, donnees=None, jeton=None, id_req=None):
    """Construit une requête client -> serveur."""
    return {
        "v": VERSION,
        "type": type_req,
        "id": id_req or nouvel_id(),
        "token": jeton,
        "ts": maintenant(),
        "data": donnees or {},
    }


def reponse_ok(req, donnees=None):
    """Réponse positive à une requête."""
    return {"v": VERSION, "type": req.get("type"), "id": req.get("id"),
            "ok": True, "data": donnees or {}}


def reponse_erreur(req, message):
    """Réponse négative à une requête."""
    return {"v": VERSION, "type": req.get("type"), "id": req.get("id"),
            "ok": False, "error": message}


def evenement(type_evt, donnees=None):
    """Notification serveur -> client."""
    return {"v": VERSION, "event": type_evt, "data": donnees or {}}


def encoder(message):
    """dict -> octets (UTF-8) pour publication MQTT."""
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


def decoder(octets):
    """Octets reçus -> dict. Lève ValueError si le message est invalide."""
    try:
        message = json.loads(octets.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"message illisible : {exc}") from exc
    if not isinstance(message, dict):
        raise ValueError("le message doit être un objet JSON")
    if message.get("v") != VERSION:
        raise ValueError(f"version de protocole non supportée : {message.get('v')}")
    return message


# ---------------------------------------------------------------------------
# Adresses
# ---------------------------------------------------------------------------
def nom_valide(nom):
    return bool(nom) and bool(_RE_NOM.match(nom))


def domaine_valide(domaine):
    return bool(domaine) and bool(_RE_DOMAINE.match(domaine))


def adresse(utilisateur, domaine):
    return f"{utilisateur}@{domaine}"


def decouper_adresse(addr, domaine_defaut=None):
    """
    "alice@rt.iut" -> ("alice", "rt.iut").
    Sans '@', le domaine par défaut est utilisé. Lève ValueError si invalide.
    """
    addr = (addr or "").strip().lower()
    if "@" in addr:
        utilisateur, _, domaine = addr.partition("@")
    else:
        utilisateur, domaine = addr, domaine_defaut
    if not nom_valide(utilisateur) or not domaine_valide(domaine or ""):
        raise ValueError(f"adresse invalide : {addr!r}")
    return utilisateur, domaine


def normaliser_adresse(addr, domaine_defaut=None):
    """Renvoie l'adresse complète normalisée "user@domaine"."""
    return adresse(*decouper_adresse(addr, domaine_defaut))


def liste_adresses(texte, domaine_defaut=None):
    """
    Analyse un champ "À" / "Cc" saisi par l'utilisateur : adresses séparées
    par des virgules, points-virgules ou espaces. Les doublons sont éliminés
    (l'ordre est conservé).
    """
    if isinstance(texte, (list, tuple)):
        morceaux = list(texte)
    else:
        morceaux = re.split(r"[,;\s]+", texte or "")
    resultat = []
    for morceau in morceaux:
        if morceau.strip():
            addr = normaliser_adresse(morceau, domaine_defaut)
            if addr not in resultat:
                resultat.append(addr)
    return resultat


# ---------------------------------------------------------------------------
# Pièces jointes
# ---------------------------------------------------------------------------
def encoder_pj(octets):
    """Contenu binaire -> texte base64 (JSON ne transporte que du texte)."""
    return base64.b64encode(octets).decode("ascii")


def decoder_pj(texte):
    return base64.b64decode(texte.encode("ascii"))


def taille_lisible(nb_octets):
    """1536 -> '1.5 Ko'."""
    taille = float(nb_octets)
    for unite in ("o", "Ko", "Mo", "Go"):
        if taille < 1024 or unite == "Go":
            return f"{taille:.0f} {unite}" if unite == "o" else f"{taille:.1f} {unite}"
        taille /= 1024
    return f"{taille:.1f} Go"


def date_lisible(horodatage, avec_secondes=False):
    """Horodatage -> 'jj/mm/aaaa hh:mm'."""
    if not horodatage:
        return "-"
    fmt = "%d/%m/%Y %H:%M:%S" if avec_secondes else "%d/%m/%Y %H:%M"
    return time.strftime(fmt, time.localtime(horodatage))
