# Messagerie "mail like" - R309 Programmation événementielle

Deux protocoles sont disponibles, avec les mêmes comptes et les mêmes boîtes aux lettres :

| Protocole | Port | Broker ? | Pour quoi |
|---|---|---|---|
| **TCP R309 v1.4** (`PROTOCOLE_TCP.md`) | 5000 | non | protocole commun : interopérable avec les autres binômes |
| **MQTT** (Mosquitto) | 1883 | oui | version événementielle complète (PJ, Cc, listes de diffusion, présence push) |

## Installation
Rien à installer côté Python (la bibliothèque `paho-mqtt` est fournie dans `paho/`).
Il faut Tkinter (`python3-tk`), et un broker MQTT seulement pour le mode MQTT.

## Lancement rapide (protocole TCP v1.4, sans broker)
    python3 serveur.py                  # puis « Démarrer » : écoute TCP sur le port 5000
    python3 client.py                   # protocole « TCP R309 v1.4 », port 5000

    python3 serveur.py --console --sans-mqtt      # serveur sans interface ni broker
    python3 serveur.py adduser alice Martin Alice # création d'un compte en ligne de commande

Se connecter au serveur d'un autre binôme : mettre son adresse IP dans « Serveur / broker ».

## Mode MQTT
Rien à lancer à part le serveur : au démarrage, `serveur.py` regarde si un broker
écoute sur le port MQTT (1883) de la machine ; sinon il lance lui-même **Mosquitto**
s'il est installé (y compris dans `/usr/sbin`), ou à défaut le **mini-broker Python**
du projet. Il l'arrête à sa fermeture (un broker déjà lancé avant n'est pas touché).
Option `--sans-broker`, ou case à décocher dans l'IHM, pour désactiver ce comportement.

Lancement manuel du broker (facultatif) :

    ./lancer_broker.sh                  # Mosquitto (ou, s'il est absent, le mini-broker Python)
    python3 broker.py -v                # mini-broker Python de secours (sans installation)
    ./lancer_broker.sh securise         # broker avec ACL (compte "serveur"/"serveur")
    python3 serveur.py --utilisateur serveur --mot-de-passe serveur   # si broker sécurisé
    python3 client.py                   # protocole « MQTT (broker) », port 1883

Ce que le protocole v1.4 ne prévoit pas (pièces jointes, Cc, listes de diffusion,
corbeille, profil) est désactivé dans le client en mode TCP.

## Tests
    python3 -m unittest tests.test_tcp -v           # protocole TCP v1.4 (aucun broker requis)
    python3 -m unittest tests.test_integration -v   # MQTT (broker sécurisé requis)

## Fichiers
- `messagerie/protocole.py`     : protocole MQTT (topics, format JSON des requêtes/réponses/événements)
- `messagerie/protocole_tcp.py` : protocole TCP v1.4 (lignes JSON, constantes, conversions)
- `messagerie/stockage.py`      : persistance JSON (comptes, boîtes, contacts, listes, PJ)
- `messagerie/serveur_core.py`  : serveur (partie "SMTP" routage/relais + partie "IMAP" boîtes)
- `messagerie/serveur_tcp.py`   : écoute TCP v1.4 (sockets, un thread par client)
- `messagerie/client_core.py`   : client MQTT (requêtes asynchrones, Timer, sonde TCP)
- `messagerie/client_tcp.py`    : client TCP v1.4 (adaptateur : même interface que le client MQTT)
- `messagerie/gui_serveur.py`   : IHM du serveur (journal, utilisateurs, sessions, listes, stats)
- `messagerie/gui_client.py`    : IHM du client (mails, répondre/répondre à tous/transférer, PJ, contacts, listes, export)
- `messagerie/theme.py`         : thèmes clair / sombre
- `messagerie/export.py`        : export d'une discussion en TXT / JSON
- `messagerie/mini_broker.py`   : mini-broker MQTT 3.1.1 en Python (si Mosquitto absent) ; `broker.py` le lance seul
- `messagerie/lanceur_broker.py`: lancement automatique du broker par le serveur
- `mosquitto/`                  : configurations Mosquitto + ACL
- `tests/`                      : tests (dont un serveur de référence v1.4 indépendant)
