"""
Protocole de messagerie R309 - version 1.4 (TCP brut), voir PROTOCOLE_TCP.md.

C'est le protocole "commun" qui permet l'interopérabilité avec les
applications des autres binômes :

  * transport : une connexion TCP persistante (port 5000 par défaut) ;
  * un message = un objet JSON UTF-8 sur UNE ligne, terminé par "\\n"
    (TCP est un flux d'octets : c'est le "\\n" qui délimite les messages) ;
  * le client envoie des commandes {"cmd": ...}, le serveur répond par des
    messages {"type": ...} ; il "pousse" aussi les nouveaux mails ("mail").

Ce module regroupe ce qui est commun au serveur (serveur_tcp.py) et au
client (client_tcp.py) : constantes, découpage des lignes, conversions.
"""

import json
import re
import time

PORT_DEFAUT = 5000
TAILLE_MAX_LIGNE = 1024 * 1024          # 1 Mo
FORMAT_DATE = "%Y-%m-%d %H:%M:%S"
DOMAINE_EMAIL = "r309.fr"               # email par défaut : <user>@r309.fr

# Commandes (client -> serveur)
CMD_LOGIN = "login"
CMD_REGISTER = "register"
CMD_SEND = "send"
CMD_INBOX = "inbox"
CMD_CONTACTS = "contacts"
CMD_ADD_CONTACT = "add_contact"
CMD_DEL_CONTACT = "del_contact"
CMD_DEL_MAIL = "del_mail"

# Messages (serveur -> client)
T_LOGIN_OK = "login_ok"
T_ERROR = "error"
T_INBOX = "inbox"
T_CONTACTS = "contacts"
T_MAIL = "mail"
T_MAIL_DELETED = "mail_deleted"

# Identifiant : [A-Za-z0-9_.-]{1,32}
RE_IDENTIFIANT = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")

# Nombre d'essais de connexion et délai d'inactivité avant login
ESSAIS_MAX = 5
DELAI_AVANT_LOGIN = 300


def encoder_ligne(message):
    """dict -> octets : JSON sur une seule ligne + '\\n'."""
    return (json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


class DecoupeurLignes:
    """
    Reconstitue les lignes à partir des morceaux reçus par recv() : un recv()
    peut contenir une demi-ligne, ou plusieurs lignes d'un coup.
    """

    def __init__(self):
        self.tampon = b""
        self.trop_long = False

    def ajouter(self, morceau):
        """Ajoute des octets reçus et renvoie la liste des messages (dict) complets."""
        self.tampon += morceau
        messages = []
        while b"\n" in self.tampon:
            ligne, _, self.tampon = self.tampon.partition(b"\n")
            if self.trop_long:                 # fin d'une ligne trop longue : on la jette
                self.trop_long = False
                continue
            message = decoder_ligne(ligne)
            if message is not None:
                messages.append(message)
        if len(self.tampon) > TAILLE_MAX_LIGNE:   # ligne > 1 Mo : ignorée
            self.tampon = b""
            self.trop_long = True
        return messages


def decoder_ligne(ligne):
    """Ligne reçue -> dict, ou None si la ligne n'est pas un objet JSON valide (ignorée)."""
    if not ligne.strip():
        return None
    try:
        message = json.loads(ligne.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return message if isinstance(message, dict) else None


def date_texte(horodatage):
    """Horodatage (float) -> "YYYY-MM-DD HH:MM:SS"."""
    return time.strftime(FORMAT_DATE, time.localtime(horodatage))


def date_horodatage(texte):
    """ "YYYY-MM-DD HH:MM:SS" -> horodatage (float). Tolère un nombre ou une date absente."""
    if isinstance(texte, (int, float)):
        return float(texte)
    try:
        return time.mktime(time.strptime(str(texte), FORMAT_DATE))
    except (ValueError, OverflowError):
        return time.time()


def fil_de_discussion(objet):
    """
    Le protocole v1.4 n'a pas de notion de fil : on regroupe les mails par
    objet, sans les préfixes RE: / TR: / Fwd:.
    """
    racine = re.sub(r"^\s*((re|tr|fwd?|fw)\s*:\s*)+", "", objet or "", flags=re.IGNORECASE)
    return "objet:" + racine.strip().lower()
