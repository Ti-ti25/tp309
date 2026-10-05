"""
Messagerie "mail like" événementielle basée sur MQTT (broker Mosquitto).

R309 - Programmation événementielle - BUT2 RT - IUT des Pays de l'Adour.

Organisation du paquetage (une couche = un module) :

    protocole.py     -> définition du protocole applicatif (topics MQTT, format JSON)
    stockage.py      -> persistance des comptes, boîtes aux lettres, contacts, groupes
    serveur_core.py  -> serveur de messagerie (partie "SMTP" sortante + partie "IMAP" entrante)
    client_core.py   -> bibliothèque client (connexion, requêtes asynchrones, événements)
    gui_serveur.py   -> interface Tkinter du serveur (supervision)
    gui_client.py    -> interface Tkinter du client (lecture / écriture de mails)
"""

__version__ = "1.0"
