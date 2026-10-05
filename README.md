# Messagerie "mail like" sur MQTT (Mosquitto) - R309

## Installation
    sudo apt install mosquitto mosquitto-clients python3-tk
    pip install -r requirements.txt

## Lancement
    ./lancer_broker.sh              # broker simple (anonyme)
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
- `mosquitto/`                 : configurations Mosquitto + ACL
- `tests/test_integration.py`  : tests d'intégration (broker réel requis)
