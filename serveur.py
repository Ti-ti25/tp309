#!/usr/bin/env python3
"""
Lanceur du serveur de messagerie.

    python3 serveur.py                       # interface graphique
    python3 serveur.py --console             # sans interface (terminal)
    python3 serveur.py --domaine geii.iut --hote 192.168.1.10 --demarrer
    python3 serveur.py --utilisateur serveur --mot-de-passe serveur   # broker avec ACL
"""
import argparse
import sys

parser = argparse.ArgumentParser(description="Serveur de messagerie MQTT")
parser.add_argument("--console", action="store_true", help="mode console, sans IHM")
parser.add_argument("--demarrer", action="store_true", help="IHM : démarrer immédiatement")
parser.add_argument("--domaine", default="rt.iut")
parser.add_argument("--hote", default="localhost", help="adresse du broker Mosquitto")
parser.add_argument("--port", type=int, default=1883)
parser.add_argument("--utilisateur", help="identifiant MQTT du serveur (broker avec ACL)")
parser.add_argument("--mot-de-passe", help="mot de passe MQTT du serveur")
parser.add_argument("--donnees", default="donnees", help="répertoire de stockage")
args = parser.parse_args()

if args.console:
    from messagerie import serveur_core
    sys.argv = [sys.argv[0], "--domaine", args.domaine, "--hote", args.hote,
                "--port", str(args.port), "--donnees", args.donnees]
    if args.utilisateur:
        sys.argv += ["--utilisateur", args.utilisateur,
                     "--mot-de-passe", args.mot_de_passe or ""]
    serveur_core.main()
else:
    from messagerie import gui_serveur
    gui_serveur.main(hote=args.hote, port=args.port, domaine=args.domaine,
                     identifiant=args.utilisateur, mot_de_passe=args.mot_de_passe,
                     donnees=args.donnees, demarrer=args.demarrer)
