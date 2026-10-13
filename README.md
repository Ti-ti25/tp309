# Messagerie "mail like" sur MQTT (Mosquitto) - R309

## Installation
La bibliothèque `paho-mqtt` est fournie dans le dossier `paho/` : rien à installer
côté Python, il suffit de lancer les programmes depuis le dossier du projet.
Il faut seulement un broker Mosquitto et Tkinter :

    sudo apt install mosquitto mosquitto-clients python3-tk

## Lancement
Il faut TOUJOURS un broker MQTT lancé avant le serveur et les clients
(sinon : « Broker injoignable : Connection refused »).

    ./lancer_broker.sh              # Mosquitto (ou, s'il est absent, le mini-broker Python)
    python3 broker.py -v            # mini-broker Python de secours (sans installation)
    ./lancer_broker.sh securise     # broker avec ACL (compte "serveur"/"serveur")

    python3 serveur.py              # serveur de messagerie (IHM Tkinter)
    python3 serveur.py --utilisateur serveur --mot-de-passe serveur   # si broker sécurisé
    python3 client.py               # client de messagerie (IHM Tkinter)

## Fichiers
- `messagerie/protocole.py`    : protocole (topics MQTT, format JSON des requêtes/réponses/événements)
- `messagerie/stockage.py`     : persistance JSON (comptes, boîtes, contacts, listes, PJ)
- `messagerie/serveur_core.py` : serveur (partie "SMTP" routage/relais + partie "IMAP" boîtes)
- `messagerie/client_core.py`  : bibliothèque client (requêtes asynchrones, Timer, sonde TCP)
- `messagerie/gui_serveur.py`  : IHM du serveur (journal, utilisateurs, sessions, listes, stats)
- `messagerie/gui_client.py`   : IHM du client (mails, répondre/répondre à tous/transférer, PJ, contacts, listes, export)
- `messagerie/export.py`       : export d'une discussion en TXT / JSON
- `broker.py`                  : mini-broker MQTT 3.1.1 en Python (si Mosquitto absent)
- `mosquitto/`                 : configurations Mosquitto + ACL
- `tests/test_integration.py`  : tests d'intégration (broker réel requis)
