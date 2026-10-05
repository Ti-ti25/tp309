"""
Couche de persistance du serveur de messagerie.

Toutes les données sont conservées dans des fichiers JSON, dans un répertoire
propre au domaine :

    <racine>/users.json              comptes (mot de passe haché + salé)
    <racine>/groups.json             listes de diffusion
    <racine>/mailboxes/<user>.json   boîte aux lettres d'un utilisateur
    <racine>/contacts/<user>.json    carnet d'adresses d'un utilisateur
    <racine>/attachments/<sha256>    contenu des pièces jointes (dédupliqué)

Les callbacks MQTT s'exécutent dans le thread réseau de paho tandis que
l'interface graphique du serveur lit ces mêmes données depuis le thread
Tkinter : tous les accès sont donc protégés par un verrou (RLock).
"""

import hashlib
import hmac
import json
import os
import threading

from . import protocole as P


class ErreurStockage(Exception):
    """Erreur "métier" renvoyée au client (compte existant, mail inconnu...)."""


class Stockage:
    """Accès thread-safe aux données persistantes d'un domaine."""

    def __init__(self, racine):
        self.racine = racine
        self.verrou = threading.RLock()
        for sous_rep in ("mailboxes", "contacts", "attachments"):
            os.makedirs(os.path.join(racine, sous_rep), exist_ok=True)
        self._utilisateurs = self._lire("users.json", {})
        self._groupes = self._lire("groups.json", {})
        # Cache mémoire des boîtes aux lettres déjà chargées.
        self._boites = {}

    # ------------------------------------------------------------------
    # Lecture / écriture JSON
    # ------------------------------------------------------------------
    def _chemin(self, *morceaux):
        return os.path.join(self.racine, *morceaux)

    def _lire(self, relatif, defaut):
        chemin = self._chemin(relatif)
        if not os.path.exists(chemin):
            return defaut
        with open(chemin, "r", encoding="utf-8") as fichier:
            return json.load(fichier)

    def _ecrire(self, relatif, donnees):
        """Écriture atomique : fichier temporaire puis renommage."""
        chemin = self._chemin(relatif)
        temporaire = chemin + ".tmp"
        with open(temporaire, "w", encoding="utf-8") as fichier:
            json.dump(donnees, fichier, ensure_ascii=False, indent=1)
        os.replace(temporaire, chemin)

    # ------------------------------------------------------------------
    # Comptes utilisateurs
    # ------------------------------------------------------------------
    @staticmethod
    def _hacher(mot_de_passe, sel):
        """PBKDF2-HMAC-SHA256 : on ne stocke jamais un mot de passe en clair."""
        return hashlib.pbkdf2_hmac("sha256", mot_de_passe.encode("utf-8"),
                                   bytes.fromhex(sel), 100_000).hex()

    def creer_utilisateur(self, nom_utilisateur, mot_de_passe, nom="", prenom=""):
        with self.verrou:
            if not P.nom_valide(nom_utilisateur) or nom_utilisateur in P.NOMS_RESERVES:
                raise ErreurStockage("nom d'utilisateur invalide "
                                     "(2 à 32 caractères : a-z 0-9 . _ -)")
            if nom_utilisateur in self._utilisateurs or nom_utilisateur in self._groupes:
                raise ErreurStockage("ce nom est déjà utilisé")
            if len(mot_de_passe or "") < 4:
                raise ErreurStockage("mot de passe trop court (4 caractères minimum)")
            sel = os.urandom(16).hex()
            self._utilisateurs[nom_utilisateur] = {
                "nom": nom.strip(), "prenom": prenom.strip(),
                "sel": sel, "hash": self._hacher(mot_de_passe, sel),
                "cree_le": P.maintenant(), "derniere_connexion": None,
                "derniere_ip": None,
            }
            self._ecrire("users.json", self._utilisateurs)

    def verifier_mot_de_passe(self, nom_utilisateur, mot_de_passe):
        with self.verrou:
            compte = self._utilisateurs.get(nom_utilisateur)
            if compte is None:
                return False
            attendu = self._hacher(mot_de_passe or "", compte["sel"])
            # compare_digest : comparaison en temps constant.
            return hmac.compare_digest(attendu, compte["hash"])

    def modifier_profil(self, nom_utilisateur, nom=None, prenom=None, mot_de_passe=None):
        with self.verrou:
            compte = self._utilisateurs[nom_utilisateur]
            if nom is not None:
                compte["nom"] = nom.strip()
            if prenom is not None:
                compte["prenom"] = prenom.strip()
            if mot_de_passe:
                if len(mot_de_passe) < 4:
                    raise ErreurStockage("mot de passe trop court")
                compte["sel"] = os.urandom(16).hex()
                compte["hash"] = self._hacher(mot_de_passe, compte["sel"])
            self._ecrire("users.json", self._utilisateurs)

    def noter_connexion(self, nom_utilisateur, ip):
        with self.verrou:
            compte = self._utilisateurs[nom_utilisateur]
            compte["derniere_connexion"] = P.maintenant()
            compte["derniere_ip"] = ip
            self._ecrire("users.json", self._utilisateurs)

    def utilisateur_existe(self, nom_utilisateur):
        with self.verrou:
            return nom_utilisateur in self._utilisateurs

    def profil(self, nom_utilisateur):
        """Informations publiques d'un compte (sans le hash)."""
        with self.verrou:
            compte = self._utilisateurs.get(nom_utilisateur)
            if compte is None:
                return None
            return {"username": nom_utilisateur, "nom": compte["nom"],
                    "prenom": compte["prenom"],
                    "derniere_connexion": compte["derniere_connexion"],
                    "derniere_ip": compte["derniere_ip"]}

    def liste_utilisateurs(self):
        with self.verrou:
            return [self.profil(nom) for nom in sorted(self._utilisateurs)]

    # ------------------------------------------------------------------
    # Boîtes aux lettres
    # ------------------------------------------------------------------
    def _boite(self, nom_utilisateur):
        """Charge (avec cache) la liste des mails d'un utilisateur."""
        if nom_utilisateur not in self._boites:
            self._boites[nom_utilisateur] = self._lire(
                os.path.join("mailboxes", nom_utilisateur + ".json"), [])
        return self._boites[nom_utilisateur]

    def _sauver_boite(self, nom_utilisateur):
        self._ecrire(os.path.join("mailboxes", nom_utilisateur + ".json"),
                     self._boite(nom_utilisateur))

    def deposer(self, nom_utilisateur, mail, dossier, lu=False):
        """Dépose une copie d'un mail dans un dossier de la boîte d'un utilisateur."""
        with self.verrou:
            copie = dict(mail)
            copie["id"] = P.nouvel_id()     # chaque copie a son propre identifiant
            copie["folder"] = dossier
            copie["read"] = lu
            self._boite(nom_utilisateur).append(copie)
            self._sauver_boite(nom_utilisateur)
            return copie

    def mails(self, nom_utilisateur, dossier=None):
        with self.verrou:
            return [dict(m) for m in self._boite(nom_utilisateur)
                    if dossier is None or m["folder"] == dossier]

    def mail(self, nom_utilisateur, id_mail):
        with self.verrou:
            for m in self._boite(nom_utilisateur):
                if m["id"] == id_mail:
                    return m
            raise ErreurStockage("mail introuvable")

    def modifier_mail(self, nom_utilisateur, id_mail, **champs):
        with self.verrou:
            self.mail(nom_utilisateur, id_mail).update(champs)
            self._sauver_boite(nom_utilisateur)

    def supprimer_mail(self, nom_utilisateur, id_mail):
        with self.verrou:
            boite = self._boite(nom_utilisateur)
            boite.remove(self.mail(nom_utilisateur, id_mail))
            self._sauver_boite(nom_utilisateur)

    def nb_non_lus(self, nom_utilisateur):
        with self.verrou:
            return sum(1 for m in self._boite(nom_utilisateur)
                       if m["folder"] == P.DOSSIER_RECEPTION and not m["read"])

    def nb_mails(self, nom_utilisateur):
        with self.verrou:
            return len(self._boite(nom_utilisateur))

    # ------------------------------------------------------------------
    # Pièces jointes (stockées une seule fois, identifiées par leur SHA-256)
    # ------------------------------------------------------------------
    def stocker_pj(self, octets):
        empreinte = hashlib.sha256(octets).hexdigest()
        chemin = self._chemin("attachments", empreinte)
        if not os.path.exists(chemin):
            with open(chemin, "wb") as fichier:
                fichier.write(octets)
        return empreinte

    def lire_pj(self, empreinte):
        if not all(c in "0123456789abcdef" for c in empreinte) or len(empreinte) != 64:
            raise ErreurStockage("pièce jointe invalide")
        chemin = self._chemin("attachments", empreinte)
        if not os.path.exists(chemin):
            raise ErreurStockage("pièce jointe introuvable")
        with open(chemin, "rb") as fichier:
            return fichier.read()

    def possede_pj(self, nom_utilisateur, empreinte):
        """Vrai si l'utilisateur a au moins un mail référençant cette pièce jointe."""
        with self.verrou:
            return any(pj.get("sha256") == empreinte
                       for m in self._boite(nom_utilisateur)
                       for pj in m.get("attachments", []))

    # ------------------------------------------------------------------
    # Carnet d'adresses
    # ------------------------------------------------------------------
    def contacts(self, nom_utilisateur):
        with self.verrou:
            return self._lire(os.path.join("contacts", nom_utilisateur + ".json"), [])

    def enregistrer_contact(self, nom_utilisateur, contact):
        """Ajoute ou met à jour (clé = adresse) un contact."""
        with self.verrou:
            liste = [c for c in self.contacts(nom_utilisateur)
                     if c["address"] != contact["address"]]
            liste.append(contact)
            liste.sort(key=lambda c: (c.get("alias") or c["address"]).lower())
            self._ecrire(os.path.join("contacts", nom_utilisateur + ".json"), liste)

    def supprimer_contact(self, nom_utilisateur, addr):
        with self.verrou:
            liste = [c for c in self.contacts(nom_utilisateur) if c["address"] != addr]
            self._ecrire(os.path.join("contacts", nom_utilisateur + ".json"), liste)

    # ------------------------------------------------------------------
    # Listes de diffusion (groupes)
    # ------------------------------------------------------------------
    def creer_groupe(self, nom_groupe, proprietaire, description=""):
        with self.verrou:
            if not P.nom_valide(nom_groupe) or nom_groupe in P.NOMS_RESERVES:
                raise ErreurStockage("nom de groupe invalide")
            if nom_groupe in self._groupes or nom_groupe in self._utilisateurs:
                raise ErreurStockage("ce nom est déjà utilisé")
            self._groupes[nom_groupe] = {
                "description": description.strip(), "owner": proprietaire,
                "members": [proprietaire], "cree_le": P.maintenant(),
            }
            self._ecrire("groups.json", self._groupes)

    def groupe(self, nom_groupe):
        with self.verrou:
            return self._groupes.get(nom_groupe)

    def groupes(self):
        with self.verrou:
            return {nom: dict(g, members=list(g["members"]))
                    for nom, g in sorted(self._groupes.items())}

    def abonner(self, nom_groupe, nom_utilisateur):
        with self.verrou:
            groupe = self._groupes.get(nom_groupe)
            if groupe is None:
                raise ErreurStockage("groupe inconnu")
            if nom_utilisateur not in groupe["members"]:
                groupe["members"].append(nom_utilisateur)
                self._ecrire("groups.json", self._groupes)

    def desabonner(self, nom_groupe, nom_utilisateur):
        with self.verrou:
            groupe = self._groupes.get(nom_groupe)
            if groupe is None:
                raise ErreurStockage("groupe inconnu")
            if nom_utilisateur in groupe["members"]:
                groupe["members"].remove(nom_utilisateur)
                self._ecrire("groups.json", self._groupes)

    def supprimer_groupe(self, nom_groupe, demandeur):
        with self.verrou:
            groupe = self._groupes.get(nom_groupe)
            if groupe is None:
                raise ErreurStockage("groupe inconnu")
            if groupe["owner"] != demandeur:
                raise ErreurStockage("seul le propriétaire peut supprimer le groupe")
            del self._groupes[nom_groupe]
            self._ecrire("groups.json", self._groupes)
