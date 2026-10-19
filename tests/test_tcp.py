"""
Tests du protocole commun R309 v1.4 (TCP brut) - aucun broker nécessaire.

  1. un client "étranger" écrit au socket brut, strictement d'après
     PROTOCOLE_TCP.md, dialogue avec NOTRE serveur ;
  2. NOTRE client (ClientTCP) dialogue avec un serveur de référence
     indépendant (tests/serveur_reference_v14.py) ;
  3. un client TCP et un client... TCP échangent via notre serveur, et
     ClientTCP fonctionne aussi contre notre serveur.

    python3 -m unittest tests.test_tcp -v
"""

import json
import os
import queue
import shutil
import socket
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from messagerie import protocole as P                       # noqa: E402
from messagerie.client_tcp import ClientTCP                 # noqa: E402
from messagerie.serveur_core import ServeurMessagerie       # noqa: E402
from serveur_reference_v14 import ServeurReference          # noqa: E402


def port_libre():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ClientBrut:
    """Client minimal du protocole : un socket, des lignes JSON."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.tampon = b""

    def envoyer(self, **message):
        self.sock.sendall((json.dumps(message) + "\n").encode())

    def brut(self, octets):
        self.sock.sendall(octets)

    def recevoir(self, delai=5):
        self.sock.settimeout(delai)
        while b"\n" not in self.tampon:
            morceau = self.sock.recv(65536)
            if not morceau:
                raise ConnectionError("connexion fermée")
            self.tampon += morceau
        ligne, self.tampon = self.tampon.split(b"\n", 1)
        return json.loads(ligne)

    def attendre(self, type_msg, delai=5):
        fin = time.time() + delai
        while time.time() < fin:
            message = self.recevoir(fin - time.time())
            if message.get("type") == type_msg:
                return message
        raise AssertionError(f"{type_msg} non reçu")

    def fermer(self):
        self.sock.close()


def attendre(condition, delai=5.0):
    fin = time.time() + delai
    while time.time() < fin:
        if condition():
            return True
        time.sleep(0.05)
    return False


class TestNotreServeur(unittest.TestCase):
    """Un client étranger (socket brut) contre notre serveur."""

    @classmethod
    def setUpClass(cls):
        cls.repertoire = tempfile.mkdtemp()
        cls.port = port_libre()
        cls.serveur = ServeurMessagerie("rt.iut", None, repertoire=cls.repertoire,
                                        port_tcp=cls.port)
        cls.serveur.demarrer()

    @classmethod
    def tearDownClass(cls):
        cls.serveur.arreter()
        shutil.rmtree(cls.repertoire, ignore_errors=True)

    def connecter(self, user, inscrire=False):
        client = ClientBrut(self.port)
        if inscrire:
            client.envoyer(cmd="register", user=user, password="secret",
                           nom=user.capitalize(), prenom="P" + user)
        else:
            client.envoyer(cmd="login", user=user, password="secret")
        self.assertEqual(client.recevoir()["type"], "login_ok")
        self.addCleanup(client.fermer)
        return client

    def test_1_inscription_login_erreurs(self):
        self.connecter("alice", inscrire=True)
        c = ClientBrut(self.port)
        self.addCleanup(c.fermer)
        c.envoyer(cmd="inbox")                                  # avant login
        self.assertEqual(c.recevoir()["type"], "error")
        c.brut(b"ceci n'est pas du json\n")                     # ligne ignorée
        c.envoyer(cmd="register", user="alice", password="xxxx", nom="A", prenom="B")
        self.assertEqual(c.recevoir()["type"], "error")         # déjà pris
        c.envoyer(cmd="register", user="zoe", password="xxxx", nom="Z")
        self.assertEqual(c.recevoir()["type"], "error")         # prénom manquant
        for _ in range(4):
            c.envoyer(cmd="login", user="alice", password="faux")
            self.assertEqual(c.recevoir()["type"], "error")     # connexion gardée
        c.envoyer(cmd="login", user="alice", password="secret")
        self.assertEqual(c.recevoir()["type"], "login_ok")      # 5e essai : succès
        c.envoyer(cmd="truc")
        self.assertEqual(c.recevoir()["type"], "error")         # commande inconnue

    def test_2_cinq_echecs(self):
        c = ClientBrut(self.port)
        for _ in range(5):
            c.envoyer(cmd="login", user="alice", password="faux")
            self.assertEqual(c.recevoir()["type"], "error")
        with self.assertRaises((ConnectionError, OSError)):
            c.recevoir(2)                                       # fermée après 5 essais
        c.fermer()

    def test_3_mails_contacts(self):
        alice = self.connecter("alice")
        bob = self.connecter("bob", inscrire=True)
        alice.envoyer(cmd="send", to=["inconnu"], subject="x", body="y")
        self.assertEqual(alice.recevoir()["type"], "error")
        alice.envoyer(cmd="send", to=["bob"], subject="Salut", body="Bonjour !")
        a = alice.attendre("mail")["mail"]                      # push à l'expéditeur
        b = bob.attendre("mail")["mail"]                        # push au destinataire
        self.assertEqual(a["id"], b["id"])
        self.assertEqual((b["from"], b["to"], b["subject"]), ("alice", ["bob"], "Salut"))
        time.strptime(b["date"], "%Y-%m-%d %H:%M:%S")

        bob.envoyer(cmd="inbox")
        self.assertIn(b["id"], [m["id"] for m in bob.attendre("inbox")["mails"]])

        bob.envoyer(cmd="add_contact", contact="bob")
        self.assertEqual(bob.recevoir()["type"], "error")       # soi-même
        bob.envoyer(cmd="add_contact", contact="personne")
        self.assertEqual(bob.recevoir()["type"], "error")       # inconnu
        bob.envoyer(cmd="add_contact", contact="alice")
        contacts = bob.attendre("contacts")["contacts"]
        self.assertEqual(contacts[0]["user"], "alice")
        self.assertTrue(contacts[0]["online"])
        self.assertEqual(contacts[0]["ip"], "127.0.0.1")
        self.assertEqual(contacts[0]["email"], "alice@r309.fr")

        bob.envoyer(cmd="del_mail", id=b["id"])
        self.assertEqual(bob.attendre("mail_deleted")["id"], b["id"])
        bob.envoyer(cmd="inbox")
        self.assertNotIn(b["id"], [m["id"] for m in bob.attendre("inbox")["mails"]])
        alice.envoyer(cmd="inbox")                              # alice le garde
        self.assertIn(b["id"], [m["id"] for m in alice.attendre("inbox")["mails"]])
        bob.envoyer(cmd="del_mail", id=b["id"])
        self.assertEqual(bob.recevoir()["type"], "error")       # déjà supprimé

        bob.envoyer(cmd="del_contact", contact="alice")
        self.assertEqual(bob.attendre("contacts")["contacts"], [])

    def test_4_notre_client_sur_notre_serveur(self):
        carol = ClientTCP("127.0.0.1", self.port, "rt.iut")
        evenements = queue.Queue()
        carol.on_evenement = evenements.put
        carol.connecter()
        self.addCleanup(carol.fermer)
        self.assertTrue(carol.requete_sync(P.REQ_REGISTER, {
            "username": "carol", "password": "secret", "nom": "Petit",
            "prenom": "Carol"})["ok"])
        dave = self.connecter("dave", inscrire=True)
        dave.envoyer(cmd="send", to=["carol"], subject="Coucou", body="Ça va ?")
        evt = evenements.get(timeout=5)
        self.assertEqual(evt["event"], P.EVT_NEW_MAIL)
        self.assertEqual(evt["data"]["mail"]["from"], "dave@rt.iut")
        rep = carol.requete_sync(P.REQ_SEND_MAIL, {"to": ["dave@rt.iut"],
                                                   "subject": "RE: Coucou", "body": "Oui"})
        self.assertTrue(rep["ok"], rep)
        sujets = [dave.attendre("mail")["mail"]["subject"] for _ in range(2)]
        self.assertEqual(sujets, ["Coucou", "RE: Coucou"])     # écho de son envoi, puis réponse


class TestNotreClient(unittest.TestCase):
    """Notre ClientTCP contre un serveur de référence indépendant."""

    def setUp(self):
        self.serveur = ServeurReference()
        self.addCleanup(self.serveur.arreter)

    def client(self, user, inscrire):
        c = ClientTCP("127.0.0.1", self.serveur.port, "rt.iut")
        c.evenements = queue.Queue()
        c.on_evenement = c.evenements.put
        c.connecter()
        self.addCleanup(c.fermer)
        if inscrire:
            rep = c.requete_sync(P.REQ_REGISTER, {"username": user, "password": "pw12",
                                                  "nom": "N" + user, "prenom": "P" + user})
        else:
            rep = c.requete_sync(P.REQ_LOGIN, {"username": user, "password": "pw12"})
        self.assertTrue(rep["ok"], rep)
        return c

    def test_scenario_complet(self):
        alice = self.client("alice", True)
        bob = self.client("bob", True)
        rep = alice.requete_sync(P.REQ_LOGIN, {"username": "x", "password": "y"})
        self.assertFalse(rep["ok"])                             # erreur bien associée

        rep = alice.requete_sync(P.REQ_SEND_MAIL, {"to": ["bob@rt.iut"], "cc": [],
                                                   "subject": "Projet", "body": "Démo jeudi"})
        self.assertTrue(rep["ok"], rep)
        evt = bob.evenements.get(timeout=5)
        self.assertEqual(evt["data"]["mail"]["subject"], "Projet")
        self.assertEqual(evt["data"]["unread"], 1)

        liste = bob.requete_sync(P.REQ_LIST_MAILS, {"folder": P.DOSSIER_RECEPTION})["data"]
        self.assertEqual(len(liste["mails"]), 1)
        id_mail = liste["mails"][0]["id"]                       # entier côté serveur -> texte
        mail = bob.requete_sync(P.REQ_GET_MAIL, {"mail_id": id_mail})["data"]
        self.assertEqual((mail["mail"]["body"], mail["unread"]), ("Démo jeudi", 0))
        envoyes = alice.requete_sync(P.REQ_LIST_MAILS, {"folder": P.DOSSIER_ENVOYES})["data"]
        self.assertEqual(envoyes["mails"][0]["to"], ["bob@rt.iut"])

        rep = bob.requete_sync(P.REQ_SAVE_CONTACT, {"address": "alice@rt.iut"})
        self.assertTrue(rep["ok"], rep)
        contacts = bob.requete_sync(P.REQ_GET_CONTACTS)["data"]["contacts"]
        self.assertEqual((contacts[0]["address"], contacts[0]["online"]), ("alice@rt.iut", True))
        self.assertTrue(attendre(lambda: bob.presences.get("alice@rt.iut")))

        rep = bob.requete_sync(P.REQ_SEARCH_MAILS, {"correspondent": "alice@rt.iut"})
        self.assertEqual(len(rep["data"]["mails"]), 1)          # export de discussion

        rep = bob.requete_sync(P.REQ_DELETE_MAIL, {"mail_id": id_mail})
        self.assertTrue(rep["ok"], rep)
        liste = bob.requete_sync(P.REQ_LIST_MAILS, {"folder": P.DOSSIER_RECEPTION})["data"]
        self.assertEqual(liste["mails"], [])

        rep = alice.requete_sync(P.REQ_SEND_MAIL, {"to": ["bob"], "subject": "x",
                                                   "attachments": [{"name": "a"}]})
        self.assertFalse(rep["ok"])                             # PJ : non prévu en v1.4
        rep = alice.requete_sync(P.REQ_SEND_MAIL, {"to": ["personne@rt.iut"], "subject": "x"})
        self.assertFalse(rep["ok"])                             # erreur du serveur


if __name__ == "__main__":
    unittest.main(verbosity=2)
