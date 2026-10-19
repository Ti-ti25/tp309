"""
Interface graphique (Tkinter) du client de messagerie.

Tkinter n'est PAS thread-safe : seuls les traitements exécutés dans le thread
principal (celui de mainloop) ont le droit de toucher aux widgets. Or les
réponses du serveur et les événements MQTT arrivent dans le thread réseau de
paho, et les expirations de requêtes dans des threading.Timer.

Solution mise en œuvre (classe PontThreads) :
  * les autres threads ne font que déposer (fonction, arguments) dans une
    queue.Queue (thread-safe) ;
  * dans le thread Tkinter, une méthode réarmée toutes les 50 ms par
    root.after() vide la file et exécute les fonctions : c'est donc
    toujours le thread graphique qui met à jour les widgets.

Le reste du fichier est de la programmation événementielle "classique" :
des fonctions callback associées aux widgets (command=..., bind(...)) qui
envoient des requêtes au serveur, et des callbacks de réponse qui mettent à
jour d'autres widgets.
"""

import json
import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk

from . import protocole as P
from .client_core import ClientMessagerie, charger_piece_jointe, sonder_broker
from .client_tcp import ClientTCP, sonder_serveur_tcp
from .export import exporter_json, exporter_texte
from .theme import GestionnaireTheme, c

FICHIER_PREFERENCES = os.path.join(os.path.expanduser("~"), ".messagerie_mqtt.json")

# Protocoles proposés à la connexion : libellé -> (code, port par défaut)
PROTOCOLES = {"TCP R309 v1.4 (commun)": ("tcp", 5000), "MQTT (broker)": ("mqtt", 1883)}


# ======================================================================
# Pont entre les threads réseau et le thread Tkinter
# ======================================================================
class PontThreads:
    """File d'appels différés exécutés dans le thread Tkinter."""

    def __init__(self, racine, periode_ms=50):
        self.racine = racine
        self.periode = periode_ms
        self.file = queue.Queue()
        self._pomper()

    def appeler(self, fonction, *args):
        """Peut être appelé depuis N'IMPORTE QUEL thread."""
        self.file.put((fonction, args))

    def _pomper(self):
        """Exécutée dans le thread Tkinter, réarmée périodiquement par after()."""
        try:
            while True:
                fonction, args = self.file.get_nowait()
                try:
                    fonction(*args)
                except Exception as exc:          # une erreur ne doit pas bloquer la pompe
                    print("Erreur dans un callback :", repr(exc))
        except queue.Empty:
            pass
        self.racine.after(self.periode, self._pomper)


# ======================================================================
# Petites fenêtres utilitaires
# ======================================================================
class Notification(tk.Toplevel):
    """Bulle de notification qui disparaît seule (minuterie Tkinter after())."""

    def __init__(self, maitre, titre, texte, duree_ms=4000):
        super().__init__(maitre)
        self.overrideredirect(True)                       # pas de barre de titre
        self.attributes("-topmost", True)
        self.ignorer_theme = True                          # couleurs propres à la bulle
        cadre = tk.Frame(self, bg=c("bulle"), padx=16, pady=10,
                         highlightthickness=2, highlightbackground=c("accent"))
        cadre.pack(fill="both", expand=True)
        tk.Label(cadre, text="✉  " + titre, bg=c("bulle"), fg=c("bulle_texte"),
                 font=("TkDefaultFont", 10, "bold")).pack(anchor="w")
        tk.Label(cadre, text=texte, bg=c("bulle"), fg=c("bulle_texte"), justify="left",
                 wraplength=300).pack(anchor="w", pady=(2, 0))
        self.update_idletasks()
        x = maitre.winfo_rootx() + maitre.winfo_width() - self.winfo_width() - 20
        y = maitre.winfo_rooty() + maitre.winfo_height() - self.winfo_height() - 40
        self.geometry(f"+{max(x, 0)}+{max(y, 0)}")
        self.bind("<Button-1>", lambda e: self.destroy())  # clic = fermeture
        self.after(duree_ms, self.destroy)


class DialogueInscription(simpledialog.Dialog):
    """Formulaire de création de compte."""

    def body(self, maitre):
        self.title("Créer un compte")
        self.champs = {}
        libelles = [("prenom", "Prénom"), ("nom", "Nom"), ("username", "Identifiant"),
                    ("password", "Mot de passe"), ("confirmation", "Confirmation")]
        for ligne, (cle, libelle) in enumerate(libelles):
            ttk.Label(maitre, text=libelle + " :").grid(row=ligne, column=0, sticky="e",
                                                        padx=4, pady=3)
            champ = ttk.Entry(maitre, width=28,
                              show="•" if cle in ("password", "confirmation") else "")
            champ.grid(row=ligne, column=1, padx=4, pady=3)
            self.champs[cle] = champ
        ttk.Label(maitre, text="Identifiant : 2 à 32 caractères parmi a-z 0-9 . _ -",
                  style="Doux.TLabel").grid(row=len(libelles), column=0, columnspan=2)
        return self.champs["prenom"]

    def validate(self):
        valeurs = {cle: champ.get().strip() for cle, champ in self.champs.items()}
        if not P.nom_valide(valeurs["username"].lower()):
            messagebox.showerror("Inscription", "Identifiant invalide.", parent=self)
            return False
        if len(valeurs["password"]) < 4 or valeurs["password"] != valeurs["confirmation"]:
            messagebox.showerror("Inscription", "Mots de passe vides (4 caractères min.) "
                                 "ou différents.", parent=self)
            return False
        self.resultat = valeurs
        return True

    def apply(self):
        self.result = self.resultat


class DialogueContact(simpledialog.Dialog):
    """Ajout / modification d'un contact du carnet d'adresses."""

    def __init__(self, maitre, contact=None):
        self.contact = contact or {}
        super().__init__(maitre, "Contact")

    def body(self, maitre):
        self.champs = {}
        libelles = [("address", "Adresse mail"), ("alias", "Alias"), ("prenom", "Prénom"),
                    ("nom", "Nom"), ("notes", "Notes")]
        for ligne, (cle, libelle) in enumerate(libelles):
            ttk.Label(maitre, text=libelle + " :").grid(row=ligne, column=0, sticky="e",
                                                        padx=4, pady=3)
            champ = ttk.Entry(maitre, width=34)
            champ.insert(0, self.contact.get(cle) or "")
            champ.grid(row=ligne, column=1, padx=4, pady=3)
            self.champs[cle] = champ
        if self.contact.get("address"):
            self.champs["address"].configure(state="readonly")
        return self.champs["alias"] if self.contact.get("address") else self.champs["address"]

    def validate(self):
        try:
            P.decouper_adresse(self.champs["address"].get(), "x.y")
        except ValueError:
            messagebox.showerror("Contact", "Adresse invalide.", parent=self)
            return False
        return True

    def apply(self):
        self.result = {cle: champ.get().strip() for cle, champ in self.champs.items()}


class DialogueChoixAdresses(simpledialog.Dialog):
    """Sélection multiple d'adresses (contacts + listes de diffusion)."""

    def __init__(self, maitre, adresses):
        self.adresses = adresses                 # liste de (libellé, adresse)
        super().__init__(maitre, "Choisir des destinataires")

    def body(self, maitre):
        self.liste = tk.Listbox(maitre, selectmode="extended", width=50, height=14)
        for libelle, addr in self.adresses:
            self.liste.insert("end", f"{libelle}  <{addr}>" if libelle else addr)
        self.liste.pack(fill="both", expand=True)
        self.liste.bind("<Double-Button-1>", lambda e: self.ok())
        return self.liste

    def apply(self):
        self.result = [self.adresses[i][1] for i in self.liste.curselection()]


# ======================================================================
# Fenêtre de rédaction
# ======================================================================
class FenetreRedaction(tk.Toplevel):
    """Rédaction d'un nouveau mail, d'une réponse ou d'un transfert."""

    def __init__(self, app, a=(), cc=(), objet="", corps="", pieces=(), thread_id=None,
                 in_reply_to=None, titre="Nouveau message"):
        super().__init__(app.racine)
        self.app = app
        self.thread_id, self.in_reply_to = thread_id, in_reply_to
        # Pièces jointes : nouvelles (avec "data") ou transférées (avec "sha256").
        self.pieces = list(pieces)
        self.title(titre)
        self.geometry("720x560")
        self.minsize(520, 380)

        entete = ttk.Frame(self, padding=8)
        entete.pack(fill="x")
        entete.columnconfigure(1, weight=1)
        self.var_a = tk.StringVar(value=", ".join(a))
        self.var_cc = tk.StringVar(value=", ".join(cc))
        self.var_objet = tk.StringVar(value=objet)
        for ligne, (libelle, variable) in enumerate((("À :", self.var_a), ("Cc :", self.var_cc),
                                                     ("Objet :", self.var_objet))):
            ttk.Label(entete, text=libelle).grid(row=ligne, column=0, sticky="e", padx=4, pady=2)
            champ = ttk.Entry(entete, textvariable=variable)
            champ.grid(row=ligne, column=1, sticky="ew", pady=2)
            if variable is not self.var_objet:
                ttk.Button(entete, text="Carnet…", width=8,
                           command=lambda v=variable: self.choisir_adresses(v)).grid(
                    row=ligne, column=2, padx=4)
            if ligne == 0:
                self.champ_a = champ

        corps_cadre = ttk.Frame(self, padding=(8, 0))
        self.texte = tk.Text(corps_cadre, wrap="word", undo=True, height=12,
                             font=("TkFixedFont", 10))
        defil = ttk.Scrollbar(corps_cadre, command=self.texte.yview)
        self.texte.configure(yscrollcommand=defil.set)
        self.texte.pack(side="left", fill="both", expand=True)
        defil.pack(side="right", fill="y")
        self.texte.insert("1.0", corps)
        self.texte.mark_set("insert", "1.0")

        pj_cadre = ttk.LabelFrame(self, text="Pièces jointes", padding=6)
        self.liste_pj = tk.Listbox(pj_cadre, height=3)
        self.liste_pj.pack(side="left", fill="x", expand=True)
        boutons_pj = ttk.Frame(pj_cadre)
        boutons_pj.pack(side="right", padx=4)
        bouton_joindre = ttk.Button(boutons_pj, text="Joindre…", command=self.joindre)
        bouton_joindre.pack(fill="x")
        if getattr(app.client, "capacites", {}).get("pj") is False:
            bouton_joindre.configure(state="disabled")      # protocole TCP v1.4
        ttk.Button(boutons_pj, text="Retirer", command=self.retirer).pack(fill="x", pady=2)
        self.rafraichir_pj()

        bas = ttk.Frame(self, padding=8)
        self.etat = ttk.Label(bas, text="Ctrl+Entrée pour envoyer", style="Doux.TLabel")
        self.etat.pack(side="left")
        ttk.Button(bas, text="Annuler", command=self.destroy).pack(side="right")
        self.bouton_envoyer = ttk.Button(bas, text="Envoyer ✉", command=self.envoyer,
                                         style="Accent.TButton")
        self.bouton_envoyer.pack(side="right", padx=6)

        # Les cadres du bas sont placés AVANT la zone de texte extensible :
        # ainsi, si la fenêtre rétrécit, c'est le texte qui perd de la place
        # et le bouton "Envoyer" reste toujours visible.
        bas.pack(side="bottom", fill="x")
        pj_cadre.pack(side="bottom", fill="x", padx=8, pady=6)
        corps_cadre.pack(fill="both", expand=True)

        self.bind("<Control-Return>", lambda e: self.envoyer())
        self.bind("<Escape>", lambda e: self.destroy())
        (self.champ_a if not a else self.texte).focus_set()

    def choisir_adresses(self, variable):
        choix = DialogueChoixAdresses(self, self.app.adresses_connues()).result
        if choix:
            existantes = [x.strip() for x in variable.get().split(",") if x.strip()]
            variable.set(", ".join(existantes + [c for c in choix if c not in existantes]))

    def joindre(self):
        for chemin in filedialog.askopenfilenames(parent=self, title="Joindre des fichiers"):
            try:
                self.pieces.append(charger_piece_jointe(chemin))
            except (OSError, ValueError) as exc:
                messagebox.showerror("Pièce jointe", str(exc), parent=self)
        self.rafraichir_pj()

    def retirer(self):
        for index in reversed(self.liste_pj.curselection()):
            del self.pieces[index]
        self.rafraichir_pj()

    def rafraichir_pj(self):
        self.liste_pj.delete(0, "end")
        for pj in self.pieces:
            origine = " (transférée)" if "sha256" in pj and "data" not in pj else ""
            self.liste_pj.insert("end", f"📎 {pj['name']} - "
                                        f"{P.taille_lisible(pj.get('size', 0))}{origine}")

    def envoyer(self):
        domaine = self.app.client.domaine
        try:
            a = P.liste_adresses(self.var_a.get(), domaine)
            cc = P.liste_adresses(self.var_cc.get(), domaine)
        except ValueError as exc:
            messagebox.showerror("Destinataires", str(exc), parent=self)
            return
        if not a and not cc:
            messagebox.showwarning("Destinataires", "Indiquez au moins un destinataire.",
                                   parent=self)
            return
        objet = self.var_objet.get().strip()
        if not objet and not messagebox.askyesno("Objet", "Envoyer sans objet ?", parent=self):
            return
        self.bouton_envoyer.configure(state="disabled")
        self.etat.configure(text="Envoi en cours…")
        self.app.req(P.REQ_SEND_MAIL, {
            "to": a, "cc": cc, "subject": objet, "body": self.texte.get("1.0", "end-1c"),
            "attachments": self.pieces, "thread_id": self.thread_id,
            "in_reply_to": self.in_reply_to}, self.envoye, self.echec, delai=30.0)

    def envoye(self, donnees):
        if donnees.get("failed"):
            details = "\n".join(f"{e['address']} : {e['reason']}" for e in donnees["failed"])
            messagebox.showwarning("Distribution partielle",
                                   f"Non distribué à :\n{details}", parent=self)
        nb = len(donnees.get("delivered", [])) + len(donnees.get("relayed", []))
        self.app.statut(f"Message envoyé ({nb} destinataire(s)).")
        if self.app.dossier == P.DOSSIER_ENVOYES:
            self.app.actualiser()
        self.destroy()

    def echec(self, erreur):
        if self.winfo_exists():
            self.bouton_envoyer.configure(state="normal")
            self.etat.configure(text="Échec de l'envoi")
            messagebox.showerror("Envoi", erreur, parent=self)


# ======================================================================
# Application principale
# ======================================================================
class ApplicationClient:
    """Client de messagerie : écran de connexion puis fenêtre principale."""

    def __init__(self, racine):
        self.racine = racine
        self.racine.title("Messagerie R309")
        self.racine.geometry("1200x720")
        self.racine.minsize(820, 520)
        self.racine.protocol("WM_DELETE_WINDOW", self.quitter)
        # Thème clair / sombre (préférence mémorisée dans ~/.messagerie_theme.json)
        self.theme = GestionnaireTheme(racine)
        self.theme.sur_changement(self._theme_change)
        self.police_grasse = self.theme.police_grasse

        self.pont = PontThreads(racine)
        self.client = None
        self.dossier = P.DOSSIER_RECEPTION
        self.mails = []                  # en-têtes du dossier courant
        self.mail_courant = None         # mail complet affiché
        self.contacts = []
        self.annuaire = []
        self.groupes = []
        self.non_lus = 0
        self.tri = ("date", True)
        self.preferences = self._charger_preferences()
        self.cadre = None
        self.afficher_connexion()

    # ------------------------------------------------------------------
    # Communication avec le serveur
    # ------------------------------------------------------------------
    def req(self, type_req, donnees=None, sur_succes=None, sur_echec=None, delai=None):
        """
        Envoie une requête ; sur_succes(data) ou sur_echec(message) seront
        exécutés DANS LE THREAD TKINTER grâce au pont.
        """
        def reponse(rep):                      # appelée dans un thread réseau
            self.pont.appeler(self._traiter_reponse, rep, sur_succes, sur_echec)

        if self.client is None:
            return
        self.client.requete(type_req, donnees, reponse, delai or P.DELAI_REQUETE)

    def _traiter_reponse(self, rep, sur_succes, sur_echec):
        if rep.get("ok"):
            if sur_succes:
                sur_succes(rep.get("data", {}))
        elif sur_echec:
            sur_echec(rep.get("error", "erreur inconnue"))
        else:
            self.statut("Erreur : " + rep.get("error", "erreur inconnue"), c("erreur"))
            messagebox.showerror("Erreur", rep.get("error", "erreur inconnue"),
                                 parent=self.racine)

    def _brancher_callbacks(self):
        """Les callbacks réseau ne font que transmettre au thread Tkinter."""
        self.client.on_evenement = lambda e: self.pont.appeler(self.sur_evenement, e)
        self.client.on_presence = lambda p: self.pont.appeler(self.sur_presence, p)
        self.client.on_statut_serveur = lambda s: self.pont.appeler(self.sur_statut_serveur, s)
        self.client.on_connexion_mqtt = lambda ok, m: self.pont.appeler(self.sur_connexion_mqtt,
                                                                        ok, m)

    # ------------------------------------------------------------------
    # Préférences (dernière configuration utilisée)
    # ------------------------------------------------------------------
    def _charger_preferences(self):
        try:
            with open(FICHIER_PREFERENCES, encoding="utf-8") as fichier:
                return json.load(fichier)
        except (OSError, ValueError):
            return {"hote": "localhost", "port": 5000, "domaine": "rt.iut", "utilisateur": "",
                    "protocole": "tcp"}

    def _sauver_preferences(self):
        try:
            with open(FICHIER_PREFERENCES, "w", encoding="utf-8") as fichier:
                json.dump(self.preferences, fichier)
        except OSError:
            pass

    # ==================================================================
    # Écran de connexion
    # ==================================================================
    def afficher_connexion(self):
        if self.cadre:
            self.cadre.destroy()
        self.racine.config(menu="")
        self.racine.title("Messagerie R309 - connexion")
        self.cadre = ttk.Frame(self.racine)
        self.cadre.pack(fill="both", expand=True)
        self.bouton_theme_connexion = ttk.Button(self.cadre, style="Outil.TButton",
                                                 command=self.theme.basculer)
        self.bouton_theme_connexion.pack(anchor="ne", padx=10, pady=10)
        carte = ttk.Frame(self.cadre, style="Carte.TFrame", padding=(40, 30))
        carte.place(relx=0.5, rely=0.5, anchor="center")
        ttk.Label(carte, text="✉  Messagerie R309", style="CarteTitre.TLabel").grid(
            row=0, column=0, columnspan=3)
        ttk.Label(carte, text="R309 - Programmation événementielle",
                  style="CarteDoux.TLabel").grid(row=1, column=0, columnspan=3, pady=(2, 22))
        self.vars_connexion = {}
        # Choix du protocole : TCP v1.4 (commun à la promo) ou MQTT
        code = self.preferences.get("protocole")
        if code is None:                       # anciennes préférences (MQTT seulement)
            code, self.preferences["port"] = "tcp", 5000
        libelle_proto = next(l for l, (c_, _) in PROTOCOLES.items() if c_ == code)
        self.var_protocole = tk.StringVar(value=libelle_proto)
        ttk.Label(carte, text="Protocole", style="Carte.TLabel").grid(
            row=2, column=0, sticky="w", padx=(0, 14), pady=5)
        choix = ttk.Combobox(carte, textvariable=self.var_protocole, state="readonly",
                             values=list(PROTOCOLES), width=28)
        choix.grid(row=2, column=1, columnspan=2, sticky="ew", pady=4)
        choix.bind("<<ComboboxSelected>>", self._protocole_change)
        champs = [("hote", "Serveur / broker"), ("port", "Port"), ("domaine", "Domaine"),
                  ("utilisateur", "Utilisateur"), ("mot_de_passe", "Mot de passe")]
        for ligne, (cle, libelle) in enumerate(champs, 3):
            ttk.Label(carte, text=libelle, style="Carte.TLabel").grid(
                row=ligne, column=0, sticky="w", padx=(0, 14), pady=5)
            variable = tk.StringVar(value=str(self.preferences.get(cle, "")))
            champ = ttk.Entry(carte, textvariable=variable, width=30,
                              show="•" if cle == "mot_de_passe" else "")
            champ.grid(row=ligne, column=1, columnspan=2, sticky="ew", pady=4)
            champ.bind("<Return>", lambda e: self.se_connecter())
            self.vars_connexion[cle] = variable
            if cle == ("mot_de_passe" if self.preferences.get("utilisateur") else "utilisateur"):
                champ.focus_set()
        boutons = ttk.Frame(carte, style="Lecture.TFrame")
        boutons.grid(row=9, column=0, columnspan=3, pady=(20, 10))
        self.boutons_connexion = [
            ttk.Button(boutons, text="Se connecter", command=self.se_connecter,
                       style="Accent.TButton"),
            ttk.Button(boutons, text="Créer un compte…", command=self.creer_compte),
            ttk.Button(boutons, text="Tester la connexion", command=self.tester_broker)]
        for bouton in self.boutons_connexion:
            bouton.pack(side="left", padx=4)
        self.etat_connexion = ttk.Label(carte, text="", style="CarteDoux.TLabel",
                                        wraplength=380, justify="center")
        self.etat_connexion.grid(row=10, column=0, columnspan=3)
        self._theme_change()

    def _protocole_change(self, _evt=None):
        """Callback de la liste déroulante : propose le port par défaut du protocole."""
        _, port = PROTOCOLES[self.var_protocole.get()]
        if self.vars_connexion["port"].get().strip() in ("", "1883", "5000"):
            self.vars_connexion["port"].set(str(port))

    def _parametres(self):
        valeurs = {cle: var.get().strip() for cle, var in self.vars_connexion.items()}
        try:
            valeurs["port"] = int(valeurs["port"])
        except ValueError:
            raise ValueError("port invalide") from None
        valeurs["domaine"] = valeurs["domaine"].lower()
        if not P.domaine_valide(valeurs["domaine"]):
            raise ValueError("domaine invalide")
        valeurs["utilisateur"] = valeurs["utilisateur"].lower()
        valeurs["protocole"] = PROTOCOLES[self.var_protocole.get()][0]
        return valeurs

    def _activer_connexion(self, actif, message="", couleur=None):
        for bouton in self.boutons_connexion:
            bouton.configure(state="normal" if actif else "disabled")
        self.etat_connexion.configure(text=message, foreground=couleur or c("doux"))

    def tester_broker(self):
        """Sonde TCP brute dans un thread (l'IHM reste réactive pendant le test)."""
        try:
            valeurs = self._parametres()
        except ValueError as exc:
            self._activer_connexion(True, str(exc), c("erreur"))
            return
        self._activer_connexion(False, "Test de la connexion en cours…")
        sonde = sonder_serveur_tcp if valeurs["protocole"] == "tcp" else sonder_broker

        def travail():
            ok, message, latence = sonde(valeurs["hote"], valeurs["port"])
            if latence is not None:
                message += f" ({latence:.1f} ms)"
            self.pont.appeler(self._activer_connexion, True, message, c("succes") if ok else c("erreur"))

        threading.Thread(target=travail, daemon=True).start()

    def _ouvrir_client(self, valeurs, suite):
        """Connexion (bloquante) dans un thread, puis suite() dans le thread Tkinter."""
        tcp = valeurs["protocole"] == "tcp"
        self._activer_connexion(False, f"Connexion à {valeurs['hote']}:{valeurs['port']} "
                                       f"({'TCP v1.4' if tcp else 'MQTT'})…")

        def travail():
            # Même interface pour les deux clients : le reste de l'IHM ne change pas.
            classe = ClientTCP if tcp else ClientMessagerie
            client = classe(valeurs["hote"], valeurs["port"], valeurs["domaine"])
            try:
                client.connecter()
            except OSError as exc:
                message = f"{'Serveur' if tcp else 'Broker'} injoignable : {exc}"
                if isinstance(exc, ConnectionRefusedError):
                    message += ("\nAucun serveur n'écoute sur ce port : lancez "
                                "python3 serveur.py, ou indiquez l'adresse IP du serveur."
                                if tcp else
                                "\nAucun broker n'écoute sur ce port : lancez "
                                "./lancer_broker.sh (ou python3 broker.py), ou indiquez "
                                "l'adresse IP de la machine qui fait tourner le broker.")
                self.pont.appeler(self._activer_connexion, True, message, c("erreur"))
                return
            self.pont.appeler(self._client_ouvert, client, suite)

        threading.Thread(target=travail, daemon=True).start()

    def _client_ouvert(self, client, suite):
        if self.client:
            self.client.fermer()
        self.client = client
        self._brancher_callbacks()
        suite()

    def se_connecter(self):
        try:
            valeurs = self._parametres()
        except ValueError as exc:
            self._activer_connexion(True, str(exc), c("erreur"))
            return
        if not valeurs["utilisateur"] or not valeurs["mot_de_passe"]:
            self._activer_connexion(True, "Indiquez un utilisateur et un mot de passe.", c("erreur"))
            return

        def authentifier():
            self.etat_connexion.configure(text="Authentification…")
            self.client.connexion(valeurs["utilisateur"], valeurs["mot_de_passe"],
                                  lambda rep: self.pont.appeler(self._apres_login, rep, valeurs))

        self._ouvrir_client(valeurs, authentifier)

    def _apres_login(self, rep, valeurs):
        if not rep.get("ok"):
            self._activer_connexion(True, rep.get("error"), c("erreur"))
            self.client.fermer()
            self.client = None
            return
        self.preferences.update({k: valeurs[k] for k in ("hote", "port", "domaine",
                                                         "utilisateur", "protocole")})
        self._sauver_preferences()
        self.non_lus = rep["data"].get("unread", 0)
        self.afficher_principal()

    def creer_compte(self):
        try:
            valeurs = self._parametres()
        except ValueError as exc:
            self._activer_connexion(True, str(exc), c("erreur"))
            return
        saisie = DialogueInscription(self.racine).result
        if not saisie:
            return

        def inscrire():
            self.client.inscrire(saisie["username"].lower(), saisie["password"], saisie["nom"],
                                 saisie["prenom"],
                                 lambda rep: self.pont.appeler(self._apres_inscription, rep,
                                                               saisie))

        self._ouvrir_client(valeurs, inscrire)

    def _apres_inscription(self, rep, saisie):
        if not rep.get("ok"):
            self._activer_connexion(True, "Inscription refusée : " + rep.get("error", ""), c("erreur"))
            return
        self.vars_connexion["utilisateur"].set(saisie["username"].lower())
        self.vars_connexion["mot_de_passe"].set(saisie["password"])
        self._activer_connexion(True, f"Compte {rep['data']['address']} créé.", c("succes"))
        self.se_connecter()

    # ==================================================================
    # Fenêtre principale
    # ==================================================================
    def afficher_principal(self):
        self.cadre.destroy()
        self.racine.title(f"Messagerie R309 - {self.client.adresse}")
        self._creer_menu()
        self.cadre = ttk.Frame(self.racine)
        self.cadre.pack(fill="both", expand=True)
        self._creer_barre_outils()
        self.onglets = ttk.Notebook(self.cadre)
        self.onglets.pack(fill="both", expand=True, padx=6, pady=(0, 4))
        self._creer_onglet_messages()
        self._creer_onglet_contacts()
        self._creer_onglet_groupes()
        self._creer_barre_etat()
        self._raccourcis()
        self._theme_change()
        capacites = getattr(self.client, "capacites", {})
        if capacites.get("groupes") is False:
            # pas de listes de diffusion dans le protocole TCP v1.4
            self.onglets.tab(2, state="disabled")

        self.actualiser()
        self.charger_contacts()
        self.charger_annuaire()
        self.charger_groupes()
        if self.client.serveur_en_ligne is not None:
            self.sur_statut_serveur({"online": self.client.serveur_en_ligne})
        self.statut(f"Connecté en tant que {self.client.adresse} (IP {self.client.ip}).", c("succes"))

    def _creer_menu(self):
        barre = tk.Menu(self.racine)
        fichier = tk.Menu(barre, tearoff=False)
        fichier.add_command(label="Nouveau message", accelerator="Ctrl+N",
                            command=self.nouveau)
        fichier.add_command(label="Exporter le fil de discussion…",
                            command=self.exporter_fil)
        fichier.add_separator()
        fichier.add_command(label="Se déconnecter", command=self.se_deconnecter)
        fichier.add_command(label="Quitter", accelerator="Ctrl+Q", command=self.quitter)
        barre.add_cascade(label="Fichier", menu=fichier)
        message = tk.Menu(barre, tearoff=False)
        message.add_command(label="Répondre", accelerator="Ctrl+R", command=self.repondre)
        message.add_command(label="Répondre à tous", accelerator="Ctrl+Maj+R",
                            command=lambda: self.repondre(tous=True))
        message.add_command(label="Transférer", accelerator="Ctrl+T", command=self.transferer)
        message.add_separator()
        message.add_command(label="Marquer comme non lu", command=self.marquer_non_lu)
        message.add_command(label="Supprimer", accelerator="Suppr", command=self.supprimer)
        message.add_command(label="Restaurer dans la boîte de réception",
                            command=self.restaurer)
        message.add_separator()
        message.add_command(label="Actualiser", accelerator="F5", command=self.actualiser)
        barre.add_cascade(label="Message", menu=message)
        compte = tk.Menu(barre, tearoff=False)
        compte.add_command(label="Modifier mon profil…", command=self.modifier_profil)
        barre.add_cascade(label="Compte", menu=compte)
        affichage = tk.Menu(barre, tearoff=False)
        self.var_sombre = tk.BooleanVar(value=self.theme.sombre)
        affichage.add_checkbutton(label="Thème sombre", accelerator="Ctrl+D",
                                  variable=self.var_sombre, command=self.theme.basculer)
        barre.add_cascade(label="Affichage", menu=affichage)
        aide = tk.Menu(barre, tearoff=False)
        aide.add_command(label="À propos", command=self.a_propos)
        barre.add_cascade(label="Aide", menu=aide)
        self.racine.config(menu=barre)

    def _creer_barre_outils(self):
        barre = ttk.Frame(self.cadre, padding=(8, 6))
        barre.pack(fill="x")
        # Les éléments de droite sont placés EN PREMIER : si la fenêtre est
        # étroite, ce sont les boutons de gauche qui rétrécissent, pas la recherche.
        self.bouton_theme = ttk.Button(barre, style="Outil.TButton",
                                       command=self.theme.basculer)
        self.bouton_theme.pack(side="right", padx=(8, 0))
        self.var_recherche = tk.StringVar()
        recherche = ttk.Entry(barre, textvariable=self.var_recherche, width=12)
        recherche.pack(side="right", padx=4)
        ttk.Label(barre, text="🔍").pack(side="right")
        # Filtrage instantané : chaque frappe déclenche le callback.
        recherche.bind("<KeyRelease>", lambda e: self.afficher_liste())
        for texte, commande in (("✉ Nouveau", self.nouveau),
                                ("↩ Répondre", self.repondre),
                                ("↩↩ Répondre à tous", lambda: self.repondre(tous=True)),
                                ("➜ Transférer", self.transferer),
                                ("🗑 Supprimer", self.supprimer),
                                ("✱ Non lu", self.marquer_non_lu),
                                ("⟳ Actualiser", self.actualiser)):
            style = "Accent.TButton" if commande == self.nouveau else "Outil.TButton"
            ttk.Button(barre, text=texte, command=commande, style=style).pack(side="left",
                                                                              padx=2)

    def _creer_barre_etat(self):
        barre = ttk.Frame(self.racine, style="Barre.TFrame", padding=(10, 4))
        barre.pack(fill="x", side="bottom", before=self.cadre)
        self.etiquette_statut = ttk.Label(barre, text="", style="Barre.TLabel")
        self.etiquette_statut.pack(side="left")
        self.etiquette_horloge = ttk.Label(barre, text="", style="Barre.TLabel")
        self.etiquette_horloge.pack(side="right")
        self.etiquette_serveur = ttk.Label(barre, text="● serveur ?", style="Barre.TLabel",
                                           foreground=c("doux"))
        self.etiquette_serveur.pack(side="right", padx=12)
        self.barre_etat = barre
        self._horloge()

    def _horloge(self):
        """Minuterie Tkinter : met à jour l'horloge chaque seconde."""
        if self.client is None or not self.etiquette_horloge.winfo_exists():
            return
        self.etiquette_horloge.configure(text=P.date_lisible(P.maintenant(), True))
        self.racine.after(1000, self._horloge)

    def _raccourcis(self):
        r = self.racine
        r.bind("<Control-n>", lambda e: self.nouveau())
        r.bind("<Control-r>", lambda e: self.repondre())
        r.bind("<Control-R>", lambda e: self.repondre(tous=True))
        r.bind("<Control-t>", lambda e: self.transferer())
        r.bind("<F5>", lambda e: self.actualiser())
        r.bind("<Control-q>", lambda e: self.quitter())
        r.bind("<Control-d>", lambda e: self.theme.basculer())

    def _theme_change(self):
        """
        Appelée après chaque changement de thème : les "tags" des Treeview et
        des Text ne dépendent d'aucun style, il faut les recolorer à la main.
        """
        libelle = "☀ Clair" if self.theme.sombre else "🌙 Sombre"
        for nom in ("bouton_theme", "bouton_theme_connexion"):
            bouton = getattr(self, nom, None)
            if bouton is not None and bouton.winfo_exists():
                bouton.configure(text=libelle)
        if getattr(self, "var_sombre", None) is not None:
            self.var_sombre.set(self.theme.sombre)
        for nom in ("arbre_contacts", "arbre_annuaire", "arbre_groupes", "arbre_mails"):
            arbre = getattr(self, nom, None)
            if arbre is None or not arbre.winfo_exists():
                continue
            arbre.tag_configure("en_ligne", foreground=c("succes"))
            arbre.tag_configure("hors_ligne", foreground=c("doux"))
            arbre.tag_configure("abonne", foreground=c("succes"))
            arbre.tag_configure("daemon", foreground=c("erreur"))
        texte = getattr(self, "texte_lecture", None)
        if texte is not None and texte.winfo_exists():
            texte.tag_configure("citation", foreground=c("citation"))
        etiquette = getattr(self, "etiquette_serveur", None)
        if etiquette is not None and etiquette.winfo_exists() and self.client:
            if self.client.serveur_en_ligne is not None:
                self.sur_statut_serveur({"online": self.client.serveur_en_ligne})
            else:
                etiquette.configure(foreground=c("doux"))
        if getattr(self, "etiquette_statut", None) and self.etiquette_statut.winfo_exists():
            self.etiquette_statut.configure(foreground="")

    def statut(self, texte, couleur=None):
        if getattr(self, "etiquette_statut", None) and self.etiquette_statut.winfo_exists():
            self.etiquette_statut.configure(text=texte, foreground=couleur or "")

    # ------------------------------------------------------------------
    # Onglet "Messages"
    # ------------------------------------------------------------------
    def _creer_onglet_messages(self):
        onglet = ttk.Frame(self.onglets)
        self.onglets.add(onglet, text="  Messages  ")
        panneau = ttk.PanedWindow(onglet, orient="horizontal")
        panneau.pack(fill="both", expand=True)

        gauche = ttk.Frame(panneau, padding=4)
        ttk.Label(gauche, text="Dossiers", font=self.police_grasse).pack(anchor="w")
        self.liste_dossiers = tk.Listbox(gauche, exportselection=False, width=24, height=4,
                                         activestyle="none", font=("TkDefaultFont", 11))
        self.liste_dossiers.pack(fill="x", pady=4)
        self.liste_dossiers.bind("<<ListboxSelect>>", self.sur_choix_dossier)
        self.etiquette_profil = ttk.Label(gauche, text="", style="Doux.TLabel", justify="left")
        self.etiquette_profil.pack(anchor="w", pady=10)
        panneau.add(gauche, weight=0)

        droite = ttk.PanedWindow(panneau, orient="vertical")
        panneau.add(droite, weight=1)

        cadre_liste = ttk.Frame(droite)
        colonnes = ("pj", "correspondant", "objet", "date")
        self.arbre_mails = ttk.Treeview(cadre_liste, columns=colonnes, show="headings",
                                        selectmode="browse")
        for col, titre, largeur in (("pj", "📎", 30), ("correspondant", "De", 230),
                                    ("objet", "Objet", 360), ("date", "Date", 150)):
            self.arbre_mails.heading(col, text=titre, command=lambda col=col: self.trier(col))
            self.arbre_mails.column(col, width=largeur, stretch=col == "objet",
                                    anchor="center" if col == "pj" else "w")
        self.arbre_mails.tag_configure("non_lu", font=self.police_grasse)
        self.arbre_mails.tag_configure("daemon", foreground=c("erreur"))
        defil = ttk.Scrollbar(cadre_liste, command=self.arbre_mails.yview)
        self.arbre_mails.configure(yscrollcommand=defil.set)
        self.arbre_mails.pack(side="left", fill="both", expand=True)
        defil.pack(side="right", fill="y")
        self.arbre_mails.bind("<<TreeviewSelect>>", self.sur_choix_mail)
        self.arbre_mails.bind("<Delete>", lambda e: self.supprimer())
        self.arbre_mails.bind("<Button-3>", self.menu_contextuel_mail)
        droite.add(cadre_liste, weight=1)

        lecture = ttk.Frame(droite, padding=(0, 8, 0, 0))
        self.entete_lecture = ttk.Label(lecture, text="Sélectionnez un message.",
                                        justify="left", anchor="w", style="Gras.TLabel")
        self.entete_lecture.pack(fill="x", padx=4)
        self.cadre_pj = ttk.Frame(lecture)
        self.cadre_pj.pack(fill="x", pady=2)
        cadre_texte = ttk.Frame(lecture)
        cadre_texte.pack(fill="both", expand=True)
        self.texte_lecture = tk.Text(cadre_texte, wrap="word", state="disabled",
                                     font=("TkFixedFont", 10), height=10)
        defil2 = ttk.Scrollbar(cadre_texte, command=self.texte_lecture.yview)
        self.texte_lecture.configure(yscrollcommand=defil2.set)
        self.texte_lecture.pack(side="left", fill="both", expand=True)
        defil2.pack(side="right", fill="y")
        self.texte_lecture.tag_configure("citation", foreground=c("citation"))
        droite.add(lecture, weight=1)

        self.menu_mail = tk.Menu(self.racine, tearoff=False)
        for libelle, commande in (("Répondre", self.repondre),
                                  ("Répondre à tous", lambda: self.repondre(tous=True)),
                                  ("Transférer", self.transferer), (None, None),
                                  ("Marquer comme non lu", self.marquer_non_lu),
                                  ("Exporter le fil de discussion…", self.exporter_fil),
                                  ("Ajouter l'expéditeur aux contacts", self.ajouter_expediteur),
                                  (None, None), ("Supprimer", self.supprimer)):
            if libelle:
                self.menu_mail.add_command(label=libelle, command=commande)
            else:
                self.menu_mail.add_separator()
        self.rafraichir_dossiers()
        self.liste_dossiers.selection_set(0)
        self.etiquette_profil.configure(
            text=f"{self.client.profil.get('prenom', '')} {self.client.profil.get('nom', '')}\n"
                 f"{self.client.adresse}\nIP : {self.client.ip}")

    def rafraichir_dossiers(self):
        selection = self.liste_dossiers.curselection()
        self.liste_dossiers.delete(0, "end")
        for dossier in P.DOSSIERS:
            libelle = P.LIBELLES_DOSSIERS[dossier]
            if dossier == P.DOSSIER_RECEPTION and self.non_lus:
                libelle += f"  ({self.non_lus})"
            self.liste_dossiers.insert("end", libelle)
        if selection:
            self.liste_dossiers.selection_set(selection[0])
        titre = f"Messagerie R309 - {self.client.adresse}"
        self.racine.title(f"({self.non_lus}) {titre}" if self.non_lus else titre)

    def sur_choix_dossier(self, _evt=None):
        selection = self.liste_dossiers.curselection()
        if selection:
            self.dossier = P.DOSSIERS[selection[0]]
            self.actualiser()

    def actualiser(self):
        self.req(P.REQ_LIST_MAILS, {"folder": self.dossier}, self._liste_recue)

    def _liste_recue(self, donnees):
        if donnees["folder"] != self.dossier:
            return                                      # réponse d'un ancien dossier
        self.mails = donnees["mails"]
        self.non_lus = donnees.get("unread", self.non_lus)
        self.rafraichir_dossiers()
        self.afficher_liste()
        self.statut(f"{len(self.mails)} message(s) dans « {P.LIBELLES_DOSSIERS[self.dossier]} ».")

    def trier(self, colonne):
        """Clic sur un en-tête de colonne : tri (inversé au second clic)."""
        inverse = not self.tri[1] if self.tri[0] == colonne else colonne == "date"
        self.tri = (colonne, inverse)
        self.afficher_liste()

    def _correspondant(self, mail):
        if self.dossier == P.DOSSIER_ENVOYES:
            return "À : " + ", ".join(mail["to"] + mail["cc"])
        nom = mail.get("from_name") or mail["from"]
        return f"{nom} (via {mail['via']})" if mail.get("via") else nom

    def afficher_liste(self):
        filtre = self.var_recherche.get().strip().lower()
        cles = {"pj": lambda m: m["nb_attachments"], "date": lambda m: m["date"],
                "objet": lambda m: m["subject"].lower(),
                "correspondant": lambda m: self._correspondant(m).lower()}
        mails = sorted(self.mails, key=cles[self.tri[0]], reverse=self.tri[1])
        selection = self.arbre_mails.selection()
        self.arbre_mails.delete(*self.arbre_mails.get_children())
        self.arbre_mails.heading("correspondant",
                                 text="À" if self.dossier == P.DOSSIER_ENVOYES else "De")
        for mail in mails:
            texte = f"{mail['subject']} {mail['from']} {mail.get('from_name', '')} " \
                    f"{' '.join(mail['to'])} {mail.get('preview', '')}".lower()
            if filtre and filtre not in texte:
                continue
            tags = []
            if not mail["read"]:
                tags.append("non_lu")
            if mail["from"].startswith(P.MAILER_DAEMON + "@"):
                tags.append("daemon")
            self.arbre_mails.insert("", "end", iid=mail["id"], tags=tags, values=(
                "📎" if mail["nb_attachments"] else "", self._correspondant(mail),
                mail["subject"], P.date_lisible(mail["date"])))
        for iid in selection:
            if self.arbre_mails.exists(iid):
                self.arbre_mails.selection_set(iid)

    def entete_selectionne(self):
        selection = self.arbre_mails.selection()
        if not selection:
            return None
        return next((m for m in self.mails if m["id"] == selection[0]), None)

    def sur_choix_mail(self, _evt=None):
        entete = self.entete_selectionne()
        if entete is None:
            return
        self.req(P.REQ_GET_MAIL, {"mail_id": entete["id"]}, self._mail_recu)

    def _mail_recu(self, donnees):
        mail = donnees["mail"]
        self.mail_courant = mail
        self.non_lus = donnees.get("unread", self.non_lus)
        for entete in self.mails:
            if entete["id"] == mail["id"]:
                entete["read"] = True
        if self.arbre_mails.exists(mail["id"]):
            tags = [t for t in self.arbre_mails.item(mail["id"], "tags") if t != "non_lu"]
            self.arbre_mails.item(mail["id"], tags=tags)
        self.rafraichir_dossiers()

        expediteur = mail["from"]
        if mail.get("from_name"):
            expediteur = f"{mail['from_name']} <{mail['from']}>"
        lignes = [f"De :     {expediteur}", f"À :      {', '.join(mail['to'])}"]
        if mail.get("cc"):
            lignes.append(f"Cc :     {', '.join(mail['cc'])}")
        if mail.get("via"):
            lignes.append(f"Via :    liste de diffusion {mail['via']}")
        lignes += [f"Date :   {P.date_lisible(mail['date'], True)}",
                   f"Objet :  {mail['subject']}"]
        self.entete_lecture.configure(text="\n".join(lignes))

        for enfant in self.cadre_pj.winfo_children():
            enfant.destroy()
        for index, pj in enumerate(mail.get("attachments", [])):
            ttk.Button(self.cadre_pj,
                       text=f"📎 {pj['name']} ({P.taille_lisible(pj['size'])})",
                       command=lambda i=index, p=pj: self.enregistrer_pj(i, p)).pack(
                side="left", padx=2)

        self.texte_lecture.configure(state="normal")
        self.texte_lecture.delete("1.0", "end")
        for ligne in mail["body"].split("\n"):
            self.texte_lecture.insert("end", ligne + "\n",
                                      "citation" if ligne.startswith(">") else ())
        self.texte_lecture.configure(state="disabled")

    def enregistrer_pj(self, index, pj):
        chemin = filedialog.asksaveasfilename(parent=self.racine, initialfile=pj["name"],
                                              title="Enregistrer la pièce jointe")
        if not chemin:
            return
        self.statut(f"Téléchargement de {pj['name']}…")

        def recu(donnees):
            with open(chemin, "wb") as fichier:
                fichier.write(P.decoder_pj(donnees["data"]))
            self.statut(f"Pièce jointe enregistrée : {chemin}", c("succes"))

        self.req(P.REQ_GET_ATTACHMENT, {"mail_id": self.mail_courant["id"], "index": index},
                 recu, delai=30.0)

    def menu_contextuel_mail(self, evt):
        ligne = self.arbre_mails.identify_row(evt.y)
        if ligne:
            self.arbre_mails.selection_set(ligne)
            self.menu_mail.tk_popup(evt.x_root, evt.y_root)

    # --- actions sur les messages -------------------------------------
    def _mail_selectionne(self):
        entete = self.entete_selectionne()
        if entete is None or self.mail_courant is None or self.mail_courant["id"] != entete["id"]:
            messagebox.showinfo("Message", "Sélectionnez d'abord un message.",
                                parent=self.racine)
            return None
        return self.mail_courant

    @staticmethod
    def _prefixer(prefixe, objet):
        return objet if objet.upper().startswith(prefixe.upper()) else f"{prefixe} {objet}"

    @staticmethod
    def _citation(mail):
        entete = (f"\n\n----- Message d'origine -----\nDe : {mail['from']}\n"
                  f"Date : {P.date_lisible(mail['date'], True)}\nObjet : {mail['subject']}\n\n")
        return entete + "\n".join("> " + ligne for ligne in mail["body"].split("\n"))

    def nouveau(self, a=()):
        FenetreRedaction(self, a=a)

    def repondre(self, tous=False):
        mail = self._mail_selectionne()
        if mail is None:
            return
        moi = self.client.adresse
        if mail["folder"] == P.DOSSIER_ENVOYES:
            a, cc = list(mail["to"]), list(mail["cc"]) if tous else []
        else:
            a, cc = [mail["from"]], []
            if tous:
                a += [x for x in mail["to"] if x != moi and x not in a]
                cc = [x for x in mail["cc"] if x != moi and x not in a]
        FenetreRedaction(self, a=a, cc=cc, objet=self._prefixer("RE:", mail["subject"]),
                         corps=self._citation(mail), thread_id=mail.get("thread_id"),
                         in_reply_to=mail.get("message_id"),
                         titre="Répondre à tous" if tous else "Répondre")

    def transferer(self):
        mail = self._mail_selectionne()
        if mail is None:
            return
        corps = ("\n\n----- Message transféré -----\n"
                 f"De : {mail['from']}\nÀ : {', '.join(mail['to'])}\n"
                 f"Date : {P.date_lisible(mail['date'], True)}\nObjet : {mail['subject']}\n\n"
                 + mail["body"])
        # Les pièces jointes sont transférées par référence (empreinte SHA-256).
        FenetreRedaction(self, objet=self._prefixer("TR:", mail["subject"]), corps=corps,
                         pieces=[dict(pj) for pj in mail.get("attachments", [])],
                         titre="Transférer")

    def marquer_non_lu(self):
        entete = self.entete_selectionne()
        if entete is None:
            return

        def fait(donnees):
            entete["read"] = False
            self.non_lus = donnees.get("unread", self.non_lus)
            self.rafraichir_dossiers()
            self.afficher_liste()

        self.req(P.REQ_MARK_READ, {"mail_id": entete["id"], "read": False}, fait)

    def supprimer(self):
        entete = self.entete_selectionne()
        if entete is None:
            return
        definitif = (self.dossier == P.DOSSIER_CORBEILLE or
                     getattr(self.client, "capacites", {}).get("corbeille") is False)
        if definitif and not messagebox.askyesno(
                "Supprimer", "Supprimer définitivement ce message ?", parent=self.racine):
            return

        def fait(donnees):
            self.statut("Message supprimé définitivement." if donnees["deleted"]
                        else "Message déplacé dans la corbeille.")
            self._vider_lecture()
            self.actualiser()

        self.req(P.REQ_DELETE_MAIL, {"mail_id": entete["id"]}, fait)

    def restaurer(self):
        entete = self.entete_selectionne()
        if entete is not None:
            self.req(P.REQ_MOVE_MAIL, {"mail_id": entete["id"], "folder": P.DOSSIER_RECEPTION},
                     lambda d: self.actualiser())

    def _vider_lecture(self):
        self.mail_courant = None
        self.entete_lecture.configure(text="Sélectionnez un message.")
        for enfant in self.cadre_pj.winfo_children():
            enfant.destroy()
        self.texte_lecture.configure(state="normal")
        self.texte_lecture.delete("1.0", "end")
        self.texte_lecture.configure(state="disabled")

    def ajouter_expediteur(self):
        mail = self._mail_selectionne()
        if mail:
            self.ajouter_contact({"address": mail["from"]})

    # --- export -------------------------------------------------------
    def _exporter(self, criteres, titre, nom_fichier):
        chemin = filedialog.asksaveasfilename(
            parent=self.racine, title="Exporter la discussion", initialfile=nom_fichier,
            defaultextension=".txt",
            filetypes=[("Texte", "*.txt"), ("JSON", "*.json"), ("Tous", "*")])
        if not chemin:
            return

        def recu(donnees):
            if not donnees["mails"]:
                messagebox.showinfo("Export", "Aucun message à exporter.", parent=self.racine)
                return
            exporteur = exporter_json if chemin.lower().endswith(".json") else exporter_texte
            exporteur(donnees["mails"], chemin, titre)
            self.statut(f"{len(donnees['mails'])} message(s) exporté(s) dans {chemin}", c("succes"))

        self.req(P.REQ_SEARCH_MAILS, criteres, recu)

    def exporter_fil(self):
        mail = self._mail_selectionne()
        if mail:
            self._exporter({"thread_id": mail["thread_id"]},
                           f"Discussion : {mail['subject']}", "discussion.txt")

    # ------------------------------------------------------------------
    # Onglet "Contacts"
    # ------------------------------------------------------------------
    def _creer_arbre(self, parent, colonnes):
        cadre = ttk.Frame(parent)
        arbre = ttk.Treeview(cadre, columns=[c[0] for c in colonnes], show="headings",
                             selectmode="browse", height=8)
        for cle, titre, largeur in colonnes:
            arbre.heading(cle, text=titre)
            arbre.column(cle, width=largeur, stretch=cle in ("adresse", "description"))
        arbre.tag_configure("en_ligne", foreground=c("succes"))
        arbre.tag_configure("hors_ligne", foreground=c("doux"))
        defil = ttk.Scrollbar(cadre, command=arbre.yview)
        arbre.configure(yscrollcommand=defil.set)
        arbre.pack(side="left", fill="both", expand=True)
        defil.pack(side="right", fill="y")
        return cadre, arbre

    def _creer_onglet_contacts(self):
        onglet = ttk.Frame(self.onglets, padding=6)
        self.onglets.add(onglet, text="  Contacts  ")
        colonnes = [("statut", "Statut", 100), ("alias", "Alias", 110), ("prenom", "Prénom", 110),
                    ("nom", "Nom", 110), ("adresse", "Adresse", 200), ("ip", "IP", 110),
                    ("vu", "Dernière activité", 130)]
        haut = ttk.LabelFrame(onglet, text="Mon carnet d'adresses", padding=4)
        haut.pack(fill="both", expand=True)
        cadre, self.arbre_contacts = self._creer_arbre(haut, colonnes)
        cadre.pack(fill="both", expand=True)
        self.arbre_contacts.bind("<Double-Button-1>",
                                 lambda e: self.ecrire_a(self.arbre_contacts))
        boutons = ttk.Frame(haut)
        boutons.pack(fill="x", pady=4)
        for texte, commande in (("Ajouter…", self.ajouter_contact),
                                ("Modifier…", self.modifier_contact),
                                ("Supprimer", self.supprimer_contact),
                                ("✉ Écrire", lambda: self.ecrire_a(self.arbre_contacts)),
                                ("Exporter la discussion…", self.exporter_contact)):
            ttk.Button(boutons, text=texte, command=commande).pack(side="left", padx=2)

        bas = ttk.LabelFrame(onglet, text="Annuaire du domaine", padding=4)
        bas.pack(fill="both", expand=True, pady=(6, 0))
        cadre, self.arbre_annuaire = self._creer_arbre(bas, [c for c in colonnes
                                                             if c[0] != "alias"])
        cadre.pack(fill="both", expand=True)
        self.arbre_annuaire.bind("<Double-Button-1>",
                                 lambda e: self.ecrire_a(self.arbre_annuaire))
        boutons = ttk.Frame(bas)
        boutons.pack(fill="x", pady=4)
        ttk.Button(boutons, text="Ajouter à mes contacts",
                   command=self.annuaire_vers_contacts).pack(side="left", padx=2)
        ttk.Button(boutons, text="✉ Écrire",
                   command=lambda: self.ecrire_a(self.arbre_annuaire)).pack(side="left", padx=2)
        ttk.Button(boutons, text="⟳ Actualiser",
                   command=self.charger_annuaire).pack(side="left", padx=2)

    def _etat(self, addr, defaut=None):
        """Présence connue (topic MQTT retenu) d'une adresse."""
        return self.client.presences.get(addr) or defaut or {}

    def _valeurs_personne(self, personne, avec_alias):
        etat = self._etat(personne["address"], personne)
        en_ligne = etat.get("online")
        statut = "● en ligne" if en_ligne else ("○ hors ligne" if en_ligne is not None
                                                else "? inconnu")
        valeurs = [statut]
        if avec_alias:
            valeurs.append(personne.get("alias", ""))
        valeurs += [personne.get("prenom", ""), personne.get("nom", ""), personne["address"],
                    etat.get("ip") or "-", P.date_lisible(etat.get("last_seen"))]
        return valeurs, ("en_ligne" if en_ligne else "hors_ligne",)

    def afficher_contacts(self):
        self.arbre_contacts.delete(*self.arbre_contacts.get_children())
        for contact in self.contacts:
            valeurs, tags = self._valeurs_personne(contact, True)
            self.arbre_contacts.insert("", "end", iid=contact["address"], values=valeurs,
                                       tags=tags)
        self.arbre_annuaire.delete(*self.arbre_annuaire.get_children())
        for personne in self.annuaire:
            valeurs, tags = self._valeurs_personne(personne, False)
            self.arbre_annuaire.insert("", "end", iid=personne["address"], values=valeurs,
                                       tags=tags)

    def charger_contacts(self):
        def recu(donnees):
            self.contacts = donnees["contacts"]
            self.afficher_contacts()
        self.req(P.REQ_GET_CONTACTS, {}, recu)

    def charger_annuaire(self):
        def recu(donnees):
            self.annuaire = donnees["users"]
            self.afficher_contacts()
        self.req(P.REQ_LIST_USERS, {}, recu)

    def _adresse_selectionnee(self, arbre):
        selection = arbre.selection()
        if not selection:
            messagebox.showinfo("Contacts", "Sélectionnez d'abord une ligne.", parent=self.racine)
            return None
        return selection[0]

    def ecrire_a(self, arbre):
        addr = self._adresse_selectionnee(arbre)
        if addr:
            self.nouveau(a=[addr])

    def ajouter_contact(self, contact=None):
        """contact : champs pré-remplis éventuels (ex. adresse de l'expéditeur d'un mail)."""
        saisie = DialogueContact(self.racine, contact).result
        if saisie:
            self.req(P.REQ_SAVE_CONTACT, saisie, lambda d: (self.charger_contacts(),
                                                            self.statut("Contact enregistré.")))

    def modifier_contact(self):
        addr = self._adresse_selectionnee(self.arbre_contacts)
        contact = next((c for c in self.contacts if c["address"] == addr), None)
        if contact:
            saisie = DialogueContact(self.racine, contact).result
            if saisie:
                self.req(P.REQ_SAVE_CONTACT, saisie, lambda d: self.charger_contacts())

    def supprimer_contact(self):
        addr = self._adresse_selectionnee(self.arbre_contacts)
        if addr and messagebox.askyesno("Contacts", f"Supprimer {addr} de vos contacts ?",
                                        parent=self.racine):
            self.req(P.REQ_DELETE_CONTACT, {"address": addr}, lambda d: self.charger_contacts())

    def annuaire_vers_contacts(self):
        addr = self._adresse_selectionnee(self.arbre_annuaire)
        if addr:
            personne = next(p for p in self.annuaire if p["address"] == addr)
            saisie = DialogueContact(self.racine, {"address": addr, "nom": personne["nom"],
                                                   "prenom": personne["prenom"]}).result
            if saisie:
                self.req(P.REQ_SAVE_CONTACT, saisie, lambda d: self.charger_contacts())

    def exporter_contact(self):
        addr = self._adresse_selectionnee(self.arbre_contacts)
        if addr:
            self._exporter({"correspondent": addr}, f"Discussion avec {addr}",
                           f"discussion_{addr.split('@')[0]}.txt")

    def adresses_connues(self):
        """Contacts + listes de diffusion, pour la fenêtre de rédaction."""
        adresses = [(c.get("alias") or f"{c.get('prenom', '')} {c.get('nom', '')}".strip(),
                     c["address"]) for c in self.contacts]
        adresses += [(f"Liste : {g['name']}", g["address"]) for g in self.groupes]
        connues = {a for _, a in adresses}
        adresses += [(f"{p['prenom']} {p['nom']}".strip(), p["address"]) for p in self.annuaire
                     if p["address"] not in connues and p["address"] != self.client.adresse]
        return adresses

    # ------------------------------------------------------------------
    # Onglet "Listes de diffusion"
    # ------------------------------------------------------------------
    def _creer_onglet_groupes(self):
        onglet = ttk.Frame(self.onglets, padding=6)
        self.onglets.add(onglet, text="  Listes de diffusion  ")
        panneau = ttk.PanedWindow(onglet, orient="horizontal")
        panneau.pack(fill="both", expand=True)
        cadre, self.arbre_groupes = self._creer_arbre(panneau, [
            ("nom", "Nom", 120), ("adresse", "Adresse", 180), ("description", "Description", 220),
            ("proprietaire", "Propriétaire", 160), ("membres", "Membres", 70),
            ("abonne", "Abonné", 70)])
        self.arbre_groupes.tag_configure("abonne", foreground=c("succes"))
        self.arbre_groupes.bind("<<TreeviewSelect>>", lambda e: self.afficher_membres())
        self.arbre_groupes.bind("<Double-Button-1>",
                                lambda e: self.ecrire_a(self.arbre_groupes))
        panneau.add(cadre, weight=1)
        droite = ttk.LabelFrame(panneau, text="Membres", padding=4)
        self.liste_membres = tk.Listbox(droite, width=28)
        self.liste_membres.pack(fill="both", expand=True)
        panneau.add(droite, weight=0)
        boutons = ttk.Frame(onglet)
        boutons.pack(fill="x", pady=4)
        for texte, commande in (("Créer une liste…", self.creer_groupe),
                                ("S'abonner", lambda: self.action_groupe(P.REQ_JOIN_GROUP)),
                                ("Se désabonner", lambda: self.action_groupe(P.REQ_LEAVE_GROUP)),
                                ("Supprimer", lambda: self.action_groupe(P.REQ_DELETE_GROUP)),
                                ("✉ Écrire à la liste",
                                 lambda: self.ecrire_a(self.arbre_groupes))):
            ttk.Button(boutons, text=texte, command=commande).pack(side="left", padx=2)

    def charger_groupes(self):
        def recu(donnees):
            self.groupes = donnees["groups"]
            selection = self.arbre_groupes.selection()
            self.arbre_groupes.delete(*self.arbre_groupes.get_children())
            for g in self.groupes:
                self.arbre_groupes.insert("", "end", iid=g["address"], values=(
                    g["name"], g["address"], g["description"], g["owner"], len(g["members"]),
                    "oui" if g["member"] else "non"), tags=("abonne",) if g["member"] else ())
            for iid in selection:
                if self.arbre_groupes.exists(iid):
                    self.arbre_groupes.selection_set(iid)
            self.afficher_membres()
        self.req(P.REQ_LIST_GROUPS, {}, recu)

    def afficher_membres(self):
        self.liste_membres.delete(0, "end")
        selection = self.arbre_groupes.selection()
        groupe = next((g for g in self.groupes if selection and g["address"] == selection[0]),
                      None)
        for membre in (groupe or {}).get("members", []):
            etat = self._etat(membre)
            puce = "●" if etat.get("online") else "○"
            self.liste_membres.insert("end", f"{puce} {membre}"
                                             + (" (propriétaire)" if membre == groupe["owner"]
                                                else ""))

    def creer_groupe(self):
        nom = simpledialog.askstring("Nouvelle liste", "Nom de la liste (a-z 0-9 . _ -) :",
                                     parent=self.racine)
        if not nom:
            return
        description = simpledialog.askstring("Nouvelle liste", "Description :",
                                             parent=self.racine) or ""
        self.req(P.REQ_CREATE_GROUP, {"name": nom.strip().lower(), "description": description},
                 lambda d: self.statut(f"Liste {d['address']} créée.", c("succes")))

    def action_groupe(self, type_req):
        addr = self._adresse_selectionnee(self.arbre_groupes)
        if not addr:
            return
        if type_req == P.REQ_DELETE_GROUP and not messagebox.askyesno(
                "Supprimer", f"Supprimer la liste {addr} ?", parent=self.racine):
            return
        self.req(type_req, {"name": addr.split("@")[0]}, lambda d: self.statut("Liste mise à jour."))

    # ==================================================================
    # Événements reçus du réseau (exécutés dans le thread Tkinter via le pont)
    # ==================================================================
    def sur_evenement(self, evt):
        type_evt, donnees = evt.get("event"), evt.get("data", {})
        if self.cadre is None or not hasattr(self, "arbre_mails"):
            return
        if type_evt == P.EVT_NEW_MAIL:
            entete = donnees["mail"]
            self.non_lus = donnees.get("unread", self.non_lus + 1)
            self.rafraichir_dossiers()
            if self.dossier == P.DOSSIER_RECEPTION:
                self.mails.insert(0, entete)
                self.afficher_liste()
            self.racine.bell()
            Notification(self.racine, f"Nouveau message de {entete.get('from_name') or entete['from']}",
                         entete["subject"])
            self.statut(f"Nouveau message : « {entete['subject']} »")
        elif type_evt == P.EVT_GROUPS_CHANGED:
            self.charger_groupes()
        elif type_evt == "unread":
            if donnees.get("unread") != self.non_lus:
                self.non_lus = donnees["unread"]
                self.rafraichir_dossiers()
        elif type_evt == P.EVT_SESSION_CLOSED:
            messagebox.showwarning("Session fermée",
                                   f"Votre session a été fermée : {donnees.get('reason', '')}",
                                   parent=self.racine)
            self.retour_connexion()

    def sur_presence(self, presence):
        if not hasattr(self, "arbre_contacts") or not self.arbre_contacts.winfo_exists():
            return
        addr = presence.get("address")
        for arbre, avec_alias, source in ((self.arbre_contacts, True, self.contacts),
                                          (self.arbre_annuaire, False, self.annuaire)):
            personne = next((p for p in source if p["address"] == addr), None)
            if personne and arbre.exists(addr):
                valeurs, tags = self._valeurs_personne(personne, avec_alias)
                arbre.item(addr, values=valeurs, tags=tags)
        if addr.endswith("@" + self.client.domaine) and not any(
                p["address"] == addr for p in self.annuaire):
            self.charger_annuaire()                       # nouvel inscrit
        self.afficher_membres()
        contact = next((c for c in self.contacts if c["address"] == addr), None)
        ancien = contact.get("online") if contact else None
        if contact is not None and presence.get("online") != ancien:
            contact["online"] = presence.get("online")
            if presence.get("online") and ancien is not None:
                Notification(self.racine, "Contact connecté",
                             f"{contact.get('alias') or addr} est en ligne "
                             f"({presence.get('ip')})")

    def sur_statut_serveur(self, statut):
        if not getattr(self, "etiquette_serveur", None) or \
                not self.etiquette_serveur.winfo_exists():
            return
        if statut.get("online"):
            self.etiquette_serveur.configure(text=f"● serveur {self.client.domaine} en ligne",
                                             foreground=c("succes"))
        else:
            self.etiquette_serveur.configure(text=f"● serveur {self.client.domaine} hors ligne",
                                             foreground=c("erreur"))
            self.statut("Le serveur de messagerie est hors ligne !", c("erreur"))

    def sur_connexion_mqtt(self, connecte, message):
        self.statut(message, c("succes") if connecte else c("erreur"))

    # ==================================================================
    # Profil, déconnexion, fermeture
    # ==================================================================
    def modifier_profil(self):
        prenom = simpledialog.askstring("Profil", "Prénom :", parent=self.racine,
                                        initialvalue=self.client.profil.get("prenom", ""))
        if prenom is None:
            return
        nom = simpledialog.askstring("Profil", "Nom :", parent=self.racine,
                                     initialvalue=self.client.profil.get("nom", ""))
        if nom is None:
            return
        mot_de_passe = simpledialog.askstring(
            "Profil", "Nouveau mot de passe (laisser vide pour le conserver) :",
            parent=self.racine, show="•")

        def fait(donnees):
            self.client.profil = donnees["profile"]
            self.statut("Profil mis à jour.", c("succes"))

        self.req(P.REQ_UPDATE_PROFILE, {"prenom": prenom, "nom": nom,
                                        "password": mot_de_passe or None}, fait)

    def a_propos(self):
        messagebox.showinfo(
            "À propos",
            "Messagerie R309 - Programmation événementielle\n\n"
            + (f"Protocole : TCP R309 v1.4 (voir PROTOCOLE_TCP.md)\n"
               f"Serveur : {self.client.hote}:{self.client.port}\n"
               if getattr(self.client, "protocole", "mqtt") == "tcp" else
               f"Protocole : MQTT, applicatif v{P.VERSION}\n"
               f"Broker : {self.client.hote}:{self.client.port}\n")
            + f"Identifiant de connexion : {self.client.client_id}\n"
              f"Adresse : {self.client.adresse}", parent=self.racine)

    def retour_connexion(self):
        if self.client:
            self.client.fermer()
            self.client = None
        if hasattr(self, "barre_etat"):
            self.barre_etat.destroy()
        self.afficher_connexion()

    def se_deconnecter(self):
        if self.client and self.client.connecte:
            self.client.deconnexion(lambda rep: self.pont.appeler(self.retour_connexion))
        else:
            self.retour_connexion()

    def quitter(self):
        # Exception à la règle "jamais de requête bloquante dans Tkinter" : la
        # fenêtre se ferme, on attend au plus 1,5 s que le serveur prenne acte
        # de la déconnexion (la réponse est traitée par le thread réseau).
        if self.client and self.client.connecte:
            self.client.requete_sync(P.REQ_LOGOUT, {}, delai=1.5)
        if self.client:
            self.client.fermer()
        self.racine.destroy()


def main():
    racine = tk.Tk()
    ApplicationClient(racine)
    racine.mainloop()


if __name__ == "__main__":
    main()
