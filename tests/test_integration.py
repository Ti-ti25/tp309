"""
Tests d'intégration : deux serveurs (deux domaines) et plusieurs clients
dialoguent à travers un VRAI broker Mosquitto.

Prérequis : un broker lancé en mode sécurisé sur localhost:1883
    ./lancer_broker.sh securise
Lancement :
    python3 -m unittest discover -s tests -v

Variables d'environnement facultatives : MQTT_HOTE, MQTT_PORT, MDP_SERVEUR.
"""

import os
import queue
import shutil
import sys
import tempfile
import time
import unittest

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from messagerie import protocole as P                          # noqa: E402
from messagerie.client_core import ClientMessagerie, sonder_broker  # noqa: E402
from messagerie.export import exporter_json, exporter_texte    # noqa: E402
from messagerie.serveur_core import ServeurMessagerie          # noqa: E402

HOTE = os.environ.get("MQTT_HOTE", "localhost")
PORT = int(os.environ.get("MQTT_PORT", "1883"))
MDP = os.environ.get("MDP_SERVEUR", "serveur")
SUFFIXE = P.nouvel_id()[:6]           # domaines uniques : tests rejouables
DOM_A = f"rt{SUFFIXE}.iut"
DOM_B = f"geii{SUFFIXE}.iut"


def attendre(condition, delai=5.0):
    """Attend (activement) qu'une condition devienne vraie."""
    fin = time.time() + delai
    while time.time() < fin:
        if condition():
            return True
        time.sleep(0.05)
    return False


class ClientTest(ClientMessagerie):
    """Client qui range ses événements dans une file pour les tests."""

    def __init__(self, domaine):
        super().__init__(HOTE, PORT, domaine)
        self.evenements = queue.Queue()
        self.on_evenement = self.evenements.put

    def attendre_evenement(self, type_evt, delai=5.0):
        fin = time.time() + delai
        while time.time() < fin:
            try:
                evt = self.evenements.get(timeout=0.1)
            except queue.Empty:
                continue
            if evt.get("event") == type_evt:
                return evt
        raise AssertionError(f"événement {type_evt} non reçu")

    def ok(self, type_req, donnees=None):
        reponse = self.requete_sync(type_req, donnees)
        assert reponse.get("ok"), reponse
        return reponse["data"]


class TestMessagerie(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ok, message, _ = sonder_broker(HOTE, PORT)
        if not ok:
            raise unittest.SkipTest(message)
        cls.repertoire = tempfile.mkdtemp()
        cls.serveur_a = ServeurMessagerie(DOM_A, HOTE, PORT, "serveur", MDP, cls.repertoire)
        cls.serveur_b = ServeurMessagerie(DOM_B, HOTE, PORT, "serveur", MDP, cls.repertoire)
        cls.serveur_a.demarrer()
        cls.serveur_b.demarrer()
        assert attendre(lambda: cls.serveur_a.connecte and cls.serveur_b.connecte)
        cls.clients = []

        def nouveau(domaine, nom, prenom, utilisateur):
            client = ClientTest(domaine)
            client.connecter()
            client.ok(P.REQ_REGISTER, {"username": utilisateur, "password": "secret",
                                       "nom": nom, "prenom": prenom})
            reponse = []
            client.connexion(utilisateur, "secret", reponse.append)
            assert attendre(lambda: reponse) and reponse[0]["ok"], reponse
            cls.clients.append(client)
            return client

        cls.alice = nouveau(DOM_A, "Martin", "Alice", "alice")
        cls.bob = nouveau(DOM_A, "Durand", "Bob", "bob")
        cls.carol = nouveau(DOM_B, "Petit", "Carol", "carol")

    @classmethod
    def tearDownClass(cls):
        for client in cls.clients:
            client.fermer()
        cls.serveur_a.arreter()
        cls.serveur_b.arreter()
        shutil.rmtree(cls.repertoire, ignore_errors=True)

    # ------------------------------------------------------------------
    def test_00_sonde_tcp(self):
        ok, message, latence = sonder_broker(HOTE, PORT)
        self.assertTrue(ok, message)
        self.assertIsNotNone(latence)

    def test_01_comptes(self):
        reponse = self.alice.requete_sync(P.REQ_REGISTER, {"username": "alice",
                                                           "password": "xxxx"})
        self.assertFalse(reponse["ok"])                       # nom déjà pris
        intrus = ClientTest(DOM_A)
        intrus.connecter()
        self.clients.append(intrus)
        reponse = intrus.requete_sync(P.REQ_LOGIN, {"username": "alice", "password": "faux"})
        self.assertFalse(reponse["ok"])
        reponse = intrus.requete_sync(P.REQ_LIST_MAILS, {"folder": "reception"})
        self.assertIn("session", reponse["error"])            # requête sans session refusée

    def test_02_presence_et_annuaire(self):
        # La présence est publiée en "retained" : Bob connaît l'état d'Alice.
        self.assertTrue(attendre(lambda: self.bob.presences.get(f"alice@{DOM_A}", {})
                                 .get("online")))
        self.assertEqual(self.bob.presences[f"alice@{DOM_A}"]["ip"], self.alice.ip)
        utilisateurs = self.bob.ok(P.REQ_LIST_USERS)["users"]
        alice = next(u for u in utilisateurs if u["address"] == f"alice@{DOM_A}")
        self.assertEqual((alice["prenom"], alice["nom"], alice["online"]),
                         ("Alice", "Martin", True))
        # Présence inter-domaines (abonnement mail/+/presence/+)
        self.assertTrue(attendre(lambda: f"carol@{DOM_B}" in self.alice.presences))

    def test_03_envoi_reception_pj(self):
        contenu = bytes(range(256)) * 100
        pj = {"name": "donnees.bin", "mime": "application/octet-stream",
              "data": P.encoder_pj(contenu)}
        resultat = self.alice.ok(P.REQ_SEND_MAIL, {
            "to": ["bob"], "cc": [], "subject": "Bonjour", "body": "Salut Bob !",
            "attachments": [pj]})
        self.assertEqual(resultat["delivered"], [f"bob@{DOM_A}"])
        evt = self.bob.attendre_evenement(P.EVT_NEW_MAIL)
        entete = evt["data"]["mail"]
        self.assertEqual((entete["subject"], entete["nb_attachments"]), ("Bonjour", 1))
        self.assertGreaterEqual(evt["data"]["unread"], 1)

        mail = self.bob.ok(P.REQ_GET_MAIL, {"mail_id": entete["id"]})["mail"]
        self.assertTrue(mail["read"])
        piece = self.bob.ok(P.REQ_GET_ATTACHMENT, {"mail_id": entete["id"], "index": 0})
        self.assertEqual(P.decoder_pj(piece["data"]), contenu)

        envoyes = self.alice.ok(P.REQ_LIST_MAILS, {"folder": P.DOSSIER_ENVOYES})["mails"]
        self.assertEqual(envoyes[0]["subject"], "Bonjour")

        # Réponse (même fil) puis transfert avec la PJ par référence
        self.bob.ok(P.REQ_SEND_MAIL, {"to": [mail["from"]], "subject": "RE: Bonjour",
                                      "body": "Salut Alice", "thread_id": mail["thread_id"],
                                      "in_reply_to": mail["message_id"]})
        self.alice.attendre_evenement(P.EVT_NEW_MAIL)
        fil = self.alice.ok(P.REQ_SEARCH_MAILS, {"thread_id": mail["thread_id"]})["mails"]
        self.assertEqual([m["subject"] for m in fil], ["Bonjour", "RE: Bonjour"])

        self.bob.ok(P.REQ_SEND_MAIL, {"to": [f"carol@{DOM_B}"], "subject": "TR: Bonjour",
                                      "body": "Pour info", "attachments": [
                                          dict(mail["attachments"][0])]})
        evt = self.carol.attendre_evenement(P.EVT_NEW_MAIL)
        piece = self.carol.ok(P.REQ_GET_ATTACHMENT, {"mail_id": evt["data"]["mail"]["id"],
                                                     "index": 0})
        self.assertEqual(P.decoder_pj(piece["data"]), contenu)  # PJ relayée inter-domaines

        # Une référence vers une PJ que l'on ne possède pas est refusée
        reponse = self.carol.requete_sync(P.REQ_SEND_MAIL, {
            "to": ["carol"], "subject": "x", "attachments": [{"name": "x", "sha256": "0" * 64}]})
        self.assertFalse(reponse["ok"])

    def test_04_listes_de_diffusion(self):
        self.alice.ok(P.REQ_CREATE_GROUP, {"name": "r309", "description": "Module R309"})
        self.bob.attendre_evenement(P.EVT_GROUPS_CHANGED)
        self.bob.ok(P.REQ_JOIN_GROUP, {"name": "r309"})
        groupes = self.bob.ok(P.REQ_LIST_GROUPS)["groups"]
        r309 = next(g for g in groupes if g["name"] == "r309")
        self.assertTrue(r309["member"])
        self.assertEqual(set(r309["members"]), {f"alice@{DOM_A}", f"bob@{DOM_A}"})

        # Carol (autre domaine) écrit à la liste : relais + développement
        self.carol.ok(P.REQ_SEND_MAIL, {"to": [f"r309@{DOM_A}"], "subject": "Projet",
                                        "body": "Qui est dispo ?"})
        for client in (self.alice, self.bob):
            evt = client.attendre_evenement(P.EVT_NEW_MAIL)
            self.assertEqual(evt["data"]["mail"]["via"], f"r309@{DOM_A}")

        reponse = self.bob.requete_sync(P.REQ_DELETE_GROUP, {"name": "r309"})
        self.assertFalse(reponse["ok"])                          # pas propriétaire
        self.bob.ok(P.REQ_LEAVE_GROUP, {"name": "r309"})

    def test_05_rebonds(self):
        resultat = self.alice.ok(P.REQ_SEND_MAIL, {"to": ["personne"], "subject": "?"})
        self.assertEqual(resultat["failed"][0]["address"], f"personne@{DOM_A}")
        # Destinataire inconnu dans un autre domaine : avis asynchrone de mailer-daemon
        self.alice.ok(P.REQ_SEND_MAIL, {"to": [f"inconnu@{DOM_B}"], "subject": "Test rebond"})
        evt = self.alice.attendre_evenement(P.EVT_NEW_MAIL)
        self.assertEqual(evt["data"]["mail"]["from"], f"{P.MAILER_DAEMON}@{DOM_B}")
        self.assertIn("Test rebond", evt["data"]["mail"]["subject"])

    def test_06_contacts(self):
        self.alice.ok(P.REQ_SAVE_CONTACT, {"address": "bob", "alias": "Bobby"})
        self.alice.ok(P.REQ_SAVE_CONTACT, {"address": f"carol@{DOM_B}", "nom": "Petit",
                                           "prenom": "Carol"})
        contacts = self.alice.ok(P.REQ_GET_CONTACTS)["contacts"]
        bob = next(c for c in contacts if c["address"] == f"bob@{DOM_A}")
        self.assertEqual((bob["alias"], bob["prenom"], bob["online"]), ("Bobby", "Bob", True))
        self.alice.ok(P.REQ_DELETE_CONTACT, {"address": f"carol@{DOM_B}"})
        self.assertEqual(len(self.alice.ok(P.REQ_GET_CONTACTS)["contacts"]), 1)

    def test_07_dossiers_et_export(self):
        self.bob.ok(P.REQ_SEND_MAIL, {"to": ["alice"], "subject": "À supprimer", "body": "x"})
        mail = self.alice.attendre_evenement(P.EVT_NEW_MAIL)["data"]["mail"]
        self.alice.ok(P.REQ_MARK_READ, {"mail_id": mail["id"], "read": True})
        self.assertFalse(self.alice.ok(P.REQ_DELETE_MAIL, {"mail_id": mail["id"]})["deleted"])
        corbeille = self.alice.ok(P.REQ_LIST_MAILS, {"folder": P.DOSSIER_CORBEILLE})["mails"]
        self.assertIn(mail["id"], [m["id"] for m in corbeille])
        self.assertTrue(self.alice.ok(P.REQ_DELETE_MAIL, {"mail_id": mail["id"]})["deleted"])

        discussion = self.alice.ok(P.REQ_SEARCH_MAILS,
                                   {"correspondent": f"bob@{DOM_A}"})["mails"]
        self.assertTrue(discussion)
        repertoire = tempfile.mkdtemp()
        try:
            exporter_texte(discussion, os.path.join(repertoire, "d.txt"), "Avec Bob")
            exporter_json(discussion, os.path.join(repertoire, "d.json"), "Avec Bob")
            with open(os.path.join(repertoire, "d.txt"), encoding="utf-8") as fichier:
                self.assertIn("Salut Alice", fichier.read())
        finally:
            shutil.rmtree(repertoire)

    def test_08_acl_espion(self):
        """Avec les ACL Mosquitto, un client anonyme ne peut pas espionner mail/#."""
        recus = []
        espion = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                             client_id="espion")
        espion.on_message = lambda c, u, m: recus.append(m.topic)
        espion.connect(HOTE, PORT)
        espion.subscribe("mail/#")
        espion.loop_start()
        try:
            time.sleep(0.5)
            self.alice.ok(P.REQ_SEND_MAIL, {"to": ["bob"], "subject": "secret", "body": "chut"})
            self.bob.attendre_evenement(P.EVT_NEW_MAIL)
            time.sleep(0.3)
            interdits = [t for t in recus if "/client/" in t or "/request/" in t]
            self.assertEqual(interdits, [])
        finally:
            espion.loop_stop()
            espion.disconnect()

    def test_09_testament_et_deconnexion(self):
        # Client "planté" : socket fermé sans DISCONNECT -> le broker publie le testament
        dave = ClientTest(DOM_A)
        dave.connecter()
        dave.ok(P.REQ_REGISTER, {"username": "dave", "password": "secret"})
        reponse = []
        dave.connexion("dave", "secret", reponse.append)
        self.assertTrue(attendre(lambda: reponse and reponse[0]["ok"]))
        adresse_dave = f"dave@{DOM_A}"
        self.assertTrue(attendre(lambda: self.bob.presences.get(adresse_dave, {}).get("online")))
        dave.mqtt.loop_stop()
        dave.mqtt.socket().close()
        self.assertTrue(attendre(lambda: not self.bob.presences[adresse_dave]["online"]))

        # Déconnexion volontaire de Carol
        fini = []
        self.carol.deconnexion(fini.append)
        self.assertTrue(attendre(lambda: fini))
        self.assertTrue(attendre(
            lambda: not self.alice.presences[f"carol@{DOM_B}"]["online"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
