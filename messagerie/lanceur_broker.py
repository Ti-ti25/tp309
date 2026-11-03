"""
Lancement automatique d'un broker MQTT local par le serveur de messagerie.

Au démarrage du serveur (si le broker indiqué est sur cette machine) :
  1. si un broker écoute déjà sur le port, on l'utilise tel quel ;
  2. sinon, si Mosquitto est installé, on le lance comme sous-processus
     (subprocess.Popen), avec une petite configuration temporaire ;
  3. sinon (Mosquitto absent, pas de droits pour l'installer...), on démarre
     le mini-broker Python du projet dans un thread du serveur.

À l'arrêt du serveur, le broker est arrêté s'il a été lancé par nous
(un broker qui tournait déjà avant n'est jamais arrêté).
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time

from .mini_broker import Broker

HOTES_LOCAUX = {"", "localhost", "127.0.0.1", "0.0.0.0", "::1"}

# Sous Debian/Ubuntu, mosquitto est dans /usr/sbin (absent du PATH d'un étudiant)
CHEMINS_MOSQUITTO = ("/usr/sbin/mosquitto", "/usr/local/sbin/mosquitto",
                     "/opt/homebrew/sbin/mosquitto", "/usr/local/opt/mosquitto/sbin/mosquitto")


def est_local(hote):
    return (hote or "").strip().lower() in HOTES_LOCAUX


def port_ouvert(port, hote="127.0.0.1", delai=0.5):
    """Vrai si un programme accepte déjà les connexions TCP sur ce port."""
    try:
        with socket.create_connection((hote, int(port)), timeout=delai):
            return True
    except OSError:
        return False


def trouver_mosquitto():
    chemin = shutil.which("mosquitto")
    if chemin:
        return chemin
    return next((c for c in CHEMINS_MOSQUITTO if os.access(c, os.X_OK)), None)


class BrokerLocal:
    """Broker MQTT démarré (ou réutilisé) pour le serveur de messagerie."""

    def __init__(self, port=1883, journal=print):
        self.port = int(port)
        self.journal = journal           # fonction(message, niveau="info")
        self.processus = None            # sous-processus Mosquitto
        self.mini = None                 # mini-broker Python
        self.genre = None                # "existant", "mosquitto" ou "python"

    def demarrer(self):
        """Renvoie le genre de broker utilisé. Peut prendre jusqu'à ~3 s (Mosquitto)."""
        if port_ouvert(self.port):
            self.genre = "existant"
            self.journal(f"broker MQTT déjà actif sur le port {self.port} : il est réutilisé")
            return self.genre
        executable = trouver_mosquitto()
        if executable and self._lancer_mosquitto(executable):
            self.genre = "mosquitto"
            self.journal(f"Mosquitto lancé automatiquement sur le port {self.port} "
                         f"({executable})")
            return self.genre
        self.mini = Broker()
        self.mini.demarrer_en_arriere_plan("", self.port)    # OSError si port pris
        self.genre = "python"
        raison = "Mosquitto ne démarre pas" if executable else "Mosquitto non installé"
        self.journal(f"{raison} : mini-broker Python lancé sur le port {self.port}")
        return self.genre

    def _lancer_mosquitto(self, executable):
        repertoire = tempfile.mkdtemp(prefix="messagerie-mosquitto-")
        configuration = os.path.join(repertoire, "mosquitto.conf")
        with open(configuration, "w", encoding="utf-8") as fichier:
            # listener sans adresse = toutes les interfaces (clients des autres PC)
            fichier.write(f"listener {self.port}\nallow_anonymous true\n"
                          "message_size_limit 20000000\n")
        journal_mosquitto = open(os.path.join(repertoire, "mosquitto.log"), "wb")
        try:
            self.processus = subprocess.Popen([executable, "-c", configuration],
                                              stdout=journal_mosquitto,
                                              stderr=subprocess.STDOUT)
        except OSError:
            journal_mosquitto.close()
            return False
        journal_mosquitto.close()
        fin = time.time() + 3
        while time.time() < fin:                 # attente de l'ouverture du port
            if self.processus.poll() is not None:
                break                            # Mosquitto s'est arrêté (erreur)
            if port_ouvert(self.port):
                return True
            time.sleep(0.1)
        self.journal(f"échec du lancement de Mosquitto (voir {repertoire}/mosquitto.log)",
                     "warning")
        self._arreter_processus()
        return False

    def _arreter_processus(self):
        if self.processus and self.processus.poll() is None:
            self.processus.terminate()
            try:
                self.processus.wait(2)
            except subprocess.TimeoutExpired:
                self.processus.kill()
        self.processus = None

    def arreter(self):
        """Arrête le broker s'il a été lancé par le serveur."""
        if self.genre == "mosquitto":
            self._arreter_processus()
            self.journal("Mosquitto arrêté")
        elif self.genre == "python" and self.mini:
            self.mini.arreter()
            self.journal("mini-broker Python arrêté")
        self.genre = None
