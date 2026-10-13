"""
Thèmes graphiques (clair / sombre) communs au client et au serveur.

Tkinter mélange deux familles de widgets :
  * les widgets ttk (Button, Label, Treeview, Notebook...) dont l'apparence
    est définie de manière centralisée par un ttk.Style ;
  * les widgets "classiques" tk (Listbox, Text, Menu, Toplevel...) qui ont
    chacun leurs propres options de couleur.

Pour changer de thème EN COURS D'EXÉCUTION, GestionnaireTheme :
  1. reconfigure le ttk.Style (tous les widgets ttk changent aussitôt) ;
  2. renseigne la base d'options Tk (option_add) pour les widgets tk qui
     seront créés plus tard (fenêtres de dialogue, rédaction...) ;
  3. parcourt l'arbre des widgets existants pour recolorer les widgets tk ;
  4. prévient les écouteurs (l'application recolore ses "tags" de Treeview
     et de Text, qui ne dépendent d'aucun style).
"""

import json
import os
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk

FICHIER_THEME = os.path.join(os.path.expanduser("~"), ".messagerie_theme.json")

PALETTES = {
    "clair": {
        "fond": "#f3f5fa", "surface": "#ffffff", "surface2": "#e9edf5",
        "bordure": "#d3d9e6", "texte": "#1d2330", "doux": "#6b7487",
        "accent": "#4361ee", "accent_survol": "#3651d4", "accent_texte": "#ffffff",
        "selection": "#dce3ff", "selection_texte": "#1d2330",
        "succes": "#2b8a3e", "erreur": "#d62839", "avertissement": "#e67700",
        "citation": "#5c6bc0", "bulle": "#1d2330", "bulle_texte": "#f3f5fa",
    },
    "sombre": {
        "fond": "#181b21", "surface": "#21252d", "surface2": "#2b3039",
        "bordure": "#3a414d", "texte": "#e4e7ee", "doux": "#959eae",
        "accent": "#6d83f2", "accent_survol": "#8597f5", "accent_texte": "#ffffff",
        "selection": "#33407a", "selection_texte": "#ffffff",
        "succes": "#5fd07a", "erreur": "#ff6b6b", "avertissement": "#ffb454",
        "citation": "#9fb0ff", "bulle": "#2f3542", "bulle_texte": "#e4e7ee",
    },
}

# Palette courante (lue par c() au moment de l'appel : toujours à jour).
_courante = dict(PALETTES["clair"])


def c(nom):
    """Couleur `nom` du thème courant (ex. c("succes"))."""
    return _courante[nom]


def theme_prefere():
    try:
        with open(FICHIER_THEME, encoding="utf-8") as fichier:
            nom = json.load(fichier).get("theme", "clair")
            return nom if nom in PALETTES else "clair"
    except (OSError, ValueError):
        return "clair"


def _sauver_theme(nom):
    try:
        with open(FICHIER_THEME, "w", encoding="utf-8") as fichier:
            json.dump({"theme": nom}, fichier)
    except OSError:
        pass


def _choisir_police(candidates, defaut):
    disponibles = set(tkfont.families())
    return next((f for f in candidates if f in disponibles), defaut)


class GestionnaireTheme:
    """Applique et bascule le thème d'une application Tkinter."""

    def __init__(self, racine, nom=None):
        self.racine = racine
        self.style = ttk.Style(racine)
        self.style.theme_use("clam")       # thème ttk le plus personnalisable
        self.ecouteurs = []
        self._polices()
        self.nom = nom or theme_prefere()
        self.appliquer(self.nom)

    @property
    def sombre(self):
        return self.nom == "sombre"

    def sur_changement(self, callback):
        """callback() sera appelé après chaque changement de thème."""
        self.ecouteurs.append(callback)

    def basculer(self):
        self.appliquer("clair" if self.sombre else "sombre")
        _sauver_theme(self.nom)

    # ------------------------------------------------------------------
    def _polices(self):
        famille = _choisir_police(("Inter", "Segoe UI", "Cantarell", "Ubuntu", "Noto Sans",
                                   "DejaVu Sans"), "TkDefaultFont")
        mono = _choisir_police(("JetBrains Mono", "Ubuntu Mono", "DejaVu Sans Mono",
                                "Consolas"), "TkFixedFont")
        for nom in ("TkDefaultFont", "TkTextFont", "TkMenuFont", "TkHeadingFont",
                    "TkCaptionFont", "TkTooltipFont"):
            tkfont.nametofont(nom).configure(family=famille, size=10)
        tkfont.nametofont("TkFixedFont").configure(family=mono, size=10)
        self.police_titre = tkfont.Font(family=famille, size=18, weight="bold")
        self.police_grasse = tkfont.Font(family=famille, size=10, weight="bold")
        self.police_entete = tkfont.Font(family=famille, size=9, weight="bold")

    def appliquer(self, nom):
        self.nom = nom
        _courante.clear()
        _courante.update(PALETTES[nom])
        self._styles_ttk()
        self._options_tk()
        self.recolorer(self.racine)
        for callback in self.ecouteurs:
            callback()

    def _styles_ttk(self):
        s, p = self.style, _courante
        s.configure(".", background=p["fond"], foreground=p["texte"],
                    fieldbackground=p["surface"], bordercolor=p["bordure"],
                    lightcolor=p["fond"], darkcolor=p["fond"], troughcolor=p["surface2"],
                    focuscolor=p["accent"], selectbackground=p["selection"],
                    selectforeground=p["selection_texte"], insertcolor=p["texte"],
                    arrowcolor=p["doux"], relief="flat")
        s.map(".", foreground=[("disabled", p["doux"])])

        # Boutons : standard, "accent" (action principale) et "outil" (barre d'outils)
        s.configure("TButton", background=p["surface2"], foreground=p["texte"],
                    bordercolor=p["bordure"], lightcolor=p["surface2"],
                    darkcolor=p["surface2"], padding=(12, 5), borderwidth=1)
        s.map("TButton",
              background=[("disabled", p["fond"]), ("pressed", p["bordure"]),
                          ("active", p["selection"])],
              foreground=[("disabled", p["doux"])],
              bordercolor=[("focus", p["accent"])])
        s.configure("Accent.TButton", background=p["accent"], foreground=p["accent_texte"],
                    bordercolor=p["accent"], lightcolor=p["accent"], darkcolor=p["accent"],
                    font=self.police_grasse)
        s.map("Accent.TButton",
              background=[("disabled", p["bordure"]), ("pressed", p["accent_survol"]),
                          ("active", p["accent_survol"])],
              foreground=[("disabled", p["doux"])])
        s.configure("Outil.TButton", background=p["fond"], bordercolor=p["fond"],
                    lightcolor=p["fond"], darkcolor=p["fond"], padding=(7, 6))
        s.map("Outil.TButton", background=[("pressed", p["bordure"]),
                                           ("active", p["surface2"])],
              bordercolor=[("active", p["bordure"])])

        # Champs de saisie
        for widget in ("TEntry", "TCombobox", "TSpinbox"):
            s.configure(widget, fieldbackground=p["surface"], foreground=p["texte"],
                        bordercolor=p["bordure"], lightcolor=p["surface"],
                        darkcolor=p["surface"], padding=5)
            s.map(widget, bordercolor=[("focus", p["accent"])],
                  lightcolor=[("focus", p["accent"])],
                  fieldbackground=[("readonly", p["surface2"])])

        # Onglets
        s.configure("TNotebook", background=p["fond"], borderwidth=0,
                    tabmargins=(6, 6, 6, 0))
        s.configure("TNotebook.Tab", background=p["fond"], foreground=p["doux"],
                    padding=(16, 7), borderwidth=0, lightcolor=p["fond"],
                    bordercolor=p["fond"])
        s.map("TNotebook.Tab",
              background=[("selected", p["surface"]), ("active", p["surface2"])],
              foreground=[("selected", p["accent"]), ("active", p["texte"])],
              lightcolor=[("selected", p["accent"])])

        # Tableaux
        s.configure("Treeview", background=p["surface"], fieldbackground=p["surface"],
                    foreground=p["texte"], rowheight=28, borderwidth=0,
                    bordercolor=p["bordure"], lightcolor=p["surface"], darkcolor=p["surface"])
        s.map("Treeview", background=[("selected", p["selection"])],
              foreground=[("selected", p["selection_texte"])])
        s.configure("Treeview.Heading", background=p["surface2"], foreground=p["doux"],
                    font=self.police_entete, padding=(8, 6), borderwidth=0,
                    lightcolor=p["surface2"], darkcolor=p["surface2"],
                    bordercolor=p["bordure"])
        s.map("Treeview.Heading", background=[("active", p["bordure"])],
              foreground=[("active", p["texte"])])

        # Cadres, étiquettes, séparateurs, barres de défilement
        s.configure("TLabelframe", background=p["fond"], bordercolor=p["bordure"],
                    lightcolor=p["fond"], darkcolor=p["fond"], borderwidth=1)
        s.configure("TLabelframe.Label", background=p["fond"], foreground=p["accent"],
                    font=self.police_grasse)
        s.configure("TPanedwindow", background=p["fond"])
        s.configure("Sash", sashthickness=6, gripcount=0, background=p["fond"],
                    lightcolor=p["fond"], bordercolor=p["fond"])
        s.configure("TScrollbar", background=p["surface2"], troughcolor=p["fond"],
                    bordercolor=p["fond"], lightcolor=p["surface2"], darkcolor=p["surface2"],
                    arrowcolor=p["doux"], gripcount=0)
        s.map("TScrollbar", background=[("active", p["bordure"])])
        s.configure("TCheckbutton", background=p["fond"], indicatorbackground=p["surface"])
        s.configure("TSeparator", background=p["bordure"])

        # Styles nommés utilisés par les applications
        s.configure("Titre.TLabel", font=self.police_titre, foreground=p["accent"])
        s.configure("Gras.TLabel", font=self.police_grasse)
        s.configure("Doux.TLabel", foreground=p["doux"])
        s.configure("Carte.TFrame", background=p["surface"], bordercolor=p["bordure"],
                    relief="solid", borderwidth=1)
        s.configure("Carte.TLabel", background=p["surface"])
        s.configure("CarteDoux.TLabel", background=p["surface"], foreground=p["doux"])
        s.configure("CarteTitre.TLabel", background=p["surface"], foreground=p["accent"],
                    font=self.police_titre)
        s.configure("Barre.TFrame", background=p["surface2"])
        s.configure("Barre.TLabel", background=p["surface2"])
        s.configure("Lecture.TFrame", background=p["surface"])
        s.configure("Lecture.TLabel", background=p["surface"])

    def _options_tk(self):
        """Valeurs par défaut des widgets tk créés à partir de maintenant."""
        p, o = _courante, self.racine.option_add
        o("*Background", p["fond"], "interactive")
        o("*Foreground", p["texte"], "interactive")
        o("*activeBackground", p["selection"], "interactive")
        o("*activeForeground", p["selection_texte"], "interactive")
        o("*highlightBackground", p["fond"], "interactive")
        o("*highlightColor", p["accent"], "interactive")
        o("*selectBackground", p["selection"], "interactive")
        o("*selectForeground", p["selection_texte"], "interactive")
        o("*insertBackground", p["texte"], "interactive")
        o("*troughColor", p["surface2"], "interactive")
        for classe in ("Listbox", "Text", "Entry"):
            o(f"*{classe}.background", p["surface"], "interactive")
            o(f"*{classe}.relief", "flat", "interactive")
            o(f"*{classe}.highlightThickness", 1, "interactive")
            o(f"*{classe}.highlightBackground", p["bordure"], "interactive")
        o("*Menu.background", p["surface"], "interactive")
        o("*Menu.activeBackground", p["accent"], "interactive")
        o("*Menu.activeForeground", p["accent_texte"], "interactive")
        o("*Menu.relief", "flat", "interactive")

    def recolorer(self, widget):
        """Recolore récursivement les widgets tk déjà créés."""
        if getattr(widget, "ignorer_theme", False):
            return
        p = _courante
        classe = widget.winfo_class()
        try:
            if classe in ("Tk", "Toplevel", "Frame"):
                widget.configure(background=p["fond"])
            elif classe == "Label":
                widget.configure(background=p["fond"])
            elif classe in ("Listbox", "Text"):
                widget.configure(background=p["surface"], foreground=p["texte"],
                                 selectbackground=p["selection"],
                                 selectforeground=p["selection_texte"],
                                 highlightbackground=p["bordure"], highlightcolor=p["accent"],
                                 highlightthickness=1, relief="flat", borderwidth=0)
                if classe == "Text":
                    widget.configure(insertbackground=p["texte"], padx=8, pady=6)
            elif classe == "Menu":
                widget.configure(background=p["surface"], foreground=p["texte"],
                                 activebackground=p["accent"],
                                 activeforeground=p["accent_texte"], relief="flat",
                                 borderwidth=0)
            elif classe == "Button":
                widget.configure(background=p["surface2"], foreground=p["texte"],
                                 activebackground=p["selection"], relief="flat")
        except tk.TclError:
            pass
        for enfant in widget.winfo_children():
            self.recolorer(enfant)
