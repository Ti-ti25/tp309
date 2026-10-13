"""
Interface graphique (Tkinter) de supervision du serveur de messagerie.

Le serveur (serveur_core.ServeurMessagerie) fonctionne avec ses propres
threads ; il prévient l'IHM de ce qui se passe grâce à des "écouteurs"
(pattern Observateur). Ces écouteurs sont appelés depuis les threads du
serveur : comme côté client, ils se contentent de déposer l'information dans
une queue.Queue que le thread Tkinter vide périodiquement (root.after).
"""

import logging
import queue
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

from . import protocole as P
from .serveur_core import ServeurMessagerie
from .theme import GestionnaireTheme, c



class ApplicationServeur:
    """Fenêtre de configuration / supervision du serveur."""

    def __init__(self, racine, **defauts):
        self.racine = racine
        self.racine.title("Serveur de messagerie MQTT")
        self.racine.geometry("1050x640")
        self.racine.minsize(800, 480)
        self.racine.protocol("WM_DELETE_WINDOW", self.quitter)
        # Thème clair / sombre partagé avec le client (~/.messagerie_theme.json)
        self.theme = GestionnaireTheme(racine)
        self.theme.sur_changement(self._theme_change)
        self.serveur = None
        self.file = queue.Queue()
        self.etiquettes_stats = {}

        self._creer_configuration(defauts)
        self._creer_onglets()
        self.barre = ttk.Label(self.racine, text="Serveur arrêté.", style="Barre.TLabel",
                               padding=(10, 4))
        self.barre.pack(fill="x", side="bottom")
        self._theme_change()
        self._pomper()
        self._rafraichir_periodiquement()

    # ------------------------------------------------------------------
    # Construction de l'IHM
    # ------------------------------------------------------------------
    def _creer_configuration(self, defauts):
        cadre = ttk.LabelFrame(self.racine, text="Configuration", padding=8)
        cadre.pack(fill="x", padx=8, pady=6)
        self.vars = {}
        champs = [("hote", "Broker", 16), ("port", "Port", 6), ("domaine", "Domaine", 14),
                  ("identifiant", "Login MQTT", 10), ("mot_de_passe", "Mot de passe", 10),
                  ("donnees", "Données", 12)]
        for colonne, (cle, libelle, largeur) in enumerate(champs):
            ttk.Label(cadre, text=libelle).grid(row=0, column=colonne, sticky="w", padx=3)
            variable = tk.StringVar(value=str(defauts.get(cle) or ""))
            ttk.Entry(cadre, textvariable=variable, width=largeur,
                      show="•" if cle == "mot_de_passe" else "").grid(row=1, column=colonne,
                                                                     padx=3)
            self.vars[cle] = variable
        self.bouton_demarrer = ttk.Button(cadre, text="▶ Démarrer", command=self.demarrer,
                                          style="Accent.TButton")
        self.bouton_demarrer.grid(row=1, column=len(champs), padx=6)
        self.bouton_arreter = ttk.Button(cadre, text="■ Arrêter", command=self.arreter,
                                         state="disabled")
        self.bouton_arreter.grid(row=1, column=len(champs) + 1)
        self.voyant = tk.Label(cadre, text="●", fg=c("doux"), font=("TkDefaultFont", 16))
        self.voyant.grid(row=0, column=len(champs) + 2, rowspan=2, padx=8)
        self.bouton_theme = ttk.Button(cadre, style="Outil.TButton",
                                       command=self.theme.basculer)
        self.bouton_theme.grid(row=1, column=len(champs) + 3, padx=(4, 0))
        self.racine.bind("<Control-d>", lambda e: self.theme.basculer())

    def _arbre(self, parent, colonnes):
        cadre = ttk.Frame(parent)
        arbre = ttk.Treeview(cadre, columns=[c[0] for c in colonnes], show="headings")
        for cle, titre, largeur in colonnes:
            arbre.heading(cle, text=titre)
            arbre.column(cle, width=largeur)
        arbre.tag_configure("en_ligne", foreground=c("succes"))
        defil = ttk.Scrollbar(cadre, command=arbre.yview)
        arbre.configure(yscrollcommand=defil.set)
        arbre.pack(side="left", fill="both", expand=True)
        defil.pack(side="right", fill="y")
        cadre.pack(fill="both", expand=True)
        return arbre

    def _creer_onglets(self):
        onglets = ttk.Notebook(self.racine)
        onglets.pack(fill="both", expand=True, padx=8)

        # --- Journal ---
        journal = ttk.Frame(onglets, padding=4)
        onglets.add(journal, text="  Journal  ")
        self.texte_journal = scrolledtext.ScrolledText(journal, state="disabled", height=10,
                                                       font=("TkFixedFont", 9))
        self.texte_journal.pack(fill="both", expand=True)
        boutons = ttk.Frame(journal)
        boutons.pack(fill="x", pady=4)
        ttk.Button(boutons, text="Effacer", command=self.effacer_journal).pack(side="left")
        ttk.Button(boutons, text="Enregistrer…", command=self.enregistrer_journal).pack(
            side="left", padx=4)

        # --- Utilisateurs ---
        utilisateurs = ttk.Frame(onglets, padding=4)
        onglets.add(utilisateurs, text="  Utilisateurs  ")
        self.arbre_utilisateurs = self._arbre(utilisateurs, [
            ("login", "Login", 100), ("prenom", "Prénom", 100), ("nom", "Nom", 100),
            ("adresse", "Adresse", 180), ("statut", "Statut", 90), ("ip", "IP", 110),
            ("sessions", "Sessions", 70), ("mails", "Mails", 60), ("non_lus", "Non lus", 60),
            ("vu", "Dernière activité", 130)])

        # --- Sessions ---
        sessions = ttk.Frame(onglets, padding=4)
        onglets.add(sessions, text="  Sessions  ")
        self.arbre_sessions = self._arbre(sessions, [
            ("client_id", "Client-id MQTT", 160), ("utilisateur", "Utilisateur", 120),
            ("ip", "IP", 120), ("debut", "Connecté depuis", 140),
            ("activite", "Dernière activité", 140)])
        ttk.Button(sessions, text="Fermer la session sélectionnée",
                   command=self.expulser).pack(anchor="w", pady=4)

        # --- Listes de diffusion ---
        groupes = ttk.Frame(onglets, padding=4)
        onglets.add(groupes, text="  Listes de diffusion  ")
        self.arbre_groupes = self._arbre(groupes, [
            ("nom", "Nom", 120), ("proprietaire", "Propriétaire", 120),
            ("description", "Description", 250), ("membres", "Membres", 400)])

        # --- Statistiques ---
        stats = ttk.Frame(onglets, padding=16)
        onglets.add(stats, text="  Statistiques  ")
        libelles = [("uptime", "Démarré depuis"), ("sessions", "Sessions actives"),
                    ("requetes", "Requêtes traitées"), ("mails_envoyes", "Mails envoyés"),
                    ("mails_livres", "Mails livrés (copies)"),
                    ("mails_relayes", "Relais vers d'autres domaines"),
                    ("relais_recus", "Relais reçus"), ("rebonds", "Avis de non-distribution"),
                    ("erreurs", "Erreurs internes")]
        for ligne, (cle, libelle) in enumerate(libelles):
            ttk.Label(stats, text=libelle + " :").grid(row=ligne, column=0, sticky="e",
                                                       padx=6, pady=3)
            etiquette = ttk.Label(stats, text="-", style="Gras.TLabel")
            etiquette.grid(row=ligne, column=1, sticky="w")
            self.etiquettes_stats[cle] = etiquette

    def _theme_change(self):
        """Recolore ce qui ne dépend pas des styles ttk (tags, voyant, bouton)."""
        self.bouton_theme.configure(text="☀ Clair" if self.theme.sombre else "🌙 Sombre")
        for niveau, couleur in (("info", "texte"), ("warning", "avertissement"),
                                ("error", "erreur")):
            self.texte_journal.tag_configure(niveau, foreground=c(couleur))
        for arbre in (self.arbre_utilisateurs, self.arbre_sessions, self.arbre_groupes):
            arbre.tag_configure("en_ligne", foreground=c("succes"))
        if not self.serveur:
            self.voyant.configure(fg=c("doux"))
        else:
            self.voyant.configure(fg=c("succes") if self.serveur.connecte else c("erreur"))

    # ------------------------------------------------------------------
    # Pont threads du serveur -> thread Tkinter
    # ------------------------------------------------------------------
    def _ecouteur(self, type_evt, donnees):
        """Appelé depuis un thread du serveur : on ne touche à AUCUN widget ici."""
        self.file.put((type_evt, donnees))

    def _pomper(self):
        rafraichir = False
        try:
            while True:
                type_evt, donnees = self.file.get_nowait()
                if type_evt == "log":
                    self.journaliser(donnees["message"], donnees["niveau"])
                elif type_evt == "etat":
                    self.voyant.configure(fg=c("succes") if donnees["connecte"] else c("erreur"))
                    self.barre.configure(text="Connecté au broker." if donnees["connecte"]
                                         else "Déconnecté du broker.")
                else:
                    rafraichir = True
        except queue.Empty:
            pass
        if rafraichir:
            self.rafraichir_tables()
        self.racine.after(100, self._pomper)

    def _rafraichir_periodiquement(self):
        """Minuterie Tkinter : statistiques et tables mises à jour toutes les 2 s."""
        if self.serveur:
            self.rafraichir_stats()
            self.rafraichir_tables()
        self.racine.after(2000, self._rafraichir_periodiquement)

    # ------------------------------------------------------------------
    # Mise à jour des widgets
    # ------------------------------------------------------------------
    def journaliser(self, message, niveau="info"):
        self.texte_journal.configure(state="normal")
        self.texte_journal.insert("end", f"[{time.strftime('%H:%M:%S')}] {message}\n", niveau)
        self.texte_journal.see("end")
        self.texte_journal.configure(state="disabled")

    def effacer_journal(self):
        self.texte_journal.configure(state="normal")
        self.texte_journal.delete("1.0", "end")
        self.texte_journal.configure(state="disabled")

    def enregistrer_journal(self):
        chemin = filedialog.asksaveasfilename(defaultextension=".log",
                                              initialfile="serveur.log")
        if chemin:
            with open(chemin, "w", encoding="utf-8") as fichier:
                fichier.write(self.texte_journal.get("1.0", "end"))

    def rafraichir_stats(self):
        s = self.serveur
        duree = int(P.maintenant() - s.demarre_le) if s.demarre_le else 0
        self.etiquettes_stats["uptime"].configure(
            text=f"{duree // 3600} h {duree % 3600 // 60:02d} min {duree % 60:02d} s")
        self.etiquettes_stats["sessions"].configure(text=str(len(s.sessions)))
        for cle, valeur in s.stats.items():
            self.etiquettes_stats[cle].configure(text=str(valeur))

    def rafraichir_tables(self):
        if not self.serveur:
            return
        s = self.serveur
        # Utilisateurs
        self.arbre_utilisateurs.delete(*self.arbre_utilisateurs.get_children())
        for profil in s.stockage.liste_utilisateurs():
            etat = s.presence(profil["username"])
            self.arbre_utilisateurs.insert("", "end", values=(
                profil["username"], profil["prenom"], profil["nom"], etat["address"],
                "en ligne" if etat["online"] else "hors ligne", etat["ip"] or "-",
                etat["sessions"], s.stockage.nb_mails(profil["username"]),
                s.stockage.nb_non_lus(profil["username"]),
                P.date_lisible(etat["last_seen"], True)),
                tags=("en_ligne",) if etat["online"] else ())
        # Sessions
        selection = self.arbre_sessions.selection()
        self.arbre_sessions.delete(*self.arbre_sessions.get_children())
        for session in s.liste_sessions():
            self.arbre_sessions.insert("", "end", iid=session.client_id, values=(
                session.client_id, session.utilisateur, session.ip,
                P.date_lisible(session.debut, True),
                P.date_lisible(session.derniere_activite, True)))
        for iid in selection:
            if self.arbre_sessions.exists(iid):
                self.arbre_sessions.selection_set(iid)
        # Groupes
        self.arbre_groupes.delete(*self.arbre_groupes.get_children())
        for nom, groupe in s.stockage.groupes().items():
            self.arbre_groupes.insert("", "end", values=(
                nom, groupe["owner"], groupe["description"], ", ".join(groupe["members"])))

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def _activer(self, en_marche):
        self.bouton_demarrer.configure(state="disabled" if en_marche else "normal")
        self.bouton_arreter.configure(state="normal" if en_marche else "disabled")

    def demarrer(self):
        valeurs = {cle: var.get().strip() for cle, var in self.vars.items()}
        try:
            self.serveur = ServeurMessagerie(
                valeurs["domaine"].lower(), valeurs["hote"], int(valeurs["port"]),
                valeurs["identifiant"] or None, valeurs["mot_de_passe"] or None,
                valeurs["donnees"] or "donnees")
        except ValueError as exc:
            messagebox.showerror("Configuration", str(exc))
            return
        self.serveur.ajouter_ecouteur(self._ecouteur)
        try:
            self.serveur.demarrer()
        except OSError as exc:
            messagebox.showerror("Broker", f"Impossible de joindre le broker : {exc}")
            self.serveur = None
            return
        self._activer(True)
        self.racine.title(f"Serveur de messagerie MQTT - {self.serveur.domaine}")
        self.rafraichir_tables()

    def arreter(self):
        if self.serveur:
            self.serveur.arreter()
        self._activer(False)
        self.voyant.configure(fg=c("doux"))
        self.barre.configure(text="Serveur arrêté.")

    def expulser(self):
        selection = self.arbre_sessions.selection()
        if self.serveur and selection:
            self.serveur.expulser(selection[0])

    def quitter(self):
        self.arreter()
        self.racine.destroy()


def main(**defauts):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    racine = tk.Tk()
    application = ApplicationServeur(racine, **defauts)
    if defauts.pop("demarrer", False):
        racine.after(200, application.demarrer)
    racine.mainloop()


if __name__ == "__main__":
    main(hote="localhost", port=1883, domaine="rt.iut", donnees="donnees")
