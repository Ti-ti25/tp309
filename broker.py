#!/usr/bin/env python3
"""
Mini-broker MQTT de secours (le code est dans messagerie/mini_broker.py).

    python3 broker.py -v            # port 1883
    python3 broker.py --port 1884

Inutile en temps normal : python3 serveur.py lance lui-même un broker
(Mosquitto ou ce mini-broker) si aucun n'est déjà démarré.
"""
from messagerie.mini_broker import main

main()
