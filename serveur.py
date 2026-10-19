#!/usr/bin/env python3
"""
Lanceur du serveur de messagerie.

    python3 serveur.py                         # interface graphique (TCP v1.4 port 5000 + MQTT)
    python3 serveur.py --console               # sans interface (terminal)
    python3 serveur.py --console --sans-mqtt   # protocole TCP v1.4 seul, sans broker
    python3 serveur.py --domaine geii.iut --hote 192.168.1.10 --demarrer
    python3 serveur.py --utilisateur serveur --mot-de-passe serveur     # broker avec ACL

    python3 serveur.py adduser alice Martin Alice   # création d'un compte (protocole v1.4)
"""
import argparse
import getpass
import logging
import sys


def ajouter_utilisateur(arguments):
    """python3 serveur.py adduser <user> <nom> <prenom> [--mot-de-passe X] [--alias A] [--email E]"""
    from messagerie.stockage import ErreurStockage, Stockage

    parser = argparse.ArgumentParser(prog="serveur.py adduser",
                                     description="Création d'un compte utilisateur")
    parser.add_argument("user")
    parser.add_argument("nom")
    parser.add_argument("prenom")
    parser.add_argument("--mot-de-passe")
    parser.add_argument("--alias", default="")
    parser.add_argument("--email", default="")
    parser.add_argument("--domaine", default="rt.iut")
    parser.add_argument("--donnees", default="donnees")
    args = parser.parse_args(arguments)
    mot_de_passe = args.mot_de_passe or getpass.getpass(f"Mot de passe pour {args.user} : ")
    try:
        Stockage(f"{args.donnees}/{args.domaine}").creer_utilisateur(
            args.user.lower(), mot_de_passe, args.nom, args.prenom, args.alias, args.email)
    except ErreurStockage as exc:
        sys.exit(f"Erreur : {exc}")
    print(f"Compte « {args.user.lower()} » créé (domaine {args.domaine}). "
          "Redémarrez le serveur s'il est déjà lancé.")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "adduser":
        ajouter_utilisateur(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(description="Serveur de messagerie (TCP v1.4 + MQTT)")
    parser.add_argument("--console", action="store_true", help="mode console, sans IHM")
    parser.add_argument("--demarrer", action="store_true", help="IHM : démarrer immédiatement")
    parser.add_argument("--domaine", default="rt.iut")
    parser.add_argument("--port-tcp", type=int, default=5000, help="port du protocole TCP v1.4")
    parser.add_argument("--hote", default="localhost", help="adresse du broker MQTT")
    parser.add_argument("--port", type=int, default=1883, help="port du broker MQTT")
    parser.add_argument("--sans-mqtt", action="store_true", help="pas de broker MQTT")
    parser.add_argument("--utilisateur", help="identifiant MQTT du serveur (broker avec ACL)")
    parser.add_argument("--mot-de-passe", help="mot de passe MQTT du serveur")
    parser.add_argument("--donnees", default="donnees", help="répertoire de stockage")
    args = parser.parse_args()
    hote = "" if args.sans_mqtt else args.hote

    if args.console:
        from messagerie.serveur_core import ServeurMessagerie, lancer_console
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        lancer_console(ServeurMessagerie(args.domaine, hote, args.port, args.utilisateur,
                                         args.mot_de_passe, args.donnees, args.port_tcp))
    else:
        from messagerie import gui_serveur
        gui_serveur.main(hote=hote, port=args.port, domaine=args.domaine,
                         identifiant=args.utilisateur, mot_de_passe=args.mot_de_passe,
                         donnees=args.donnees, port_tcp=args.port_tcp, demarrer=args.demarrer)


if __name__ == "__main__":
    main()
