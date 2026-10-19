# Protocole de messagerie R309 — version 1.4 (TCP brut)

## 1. Transport

- TCP, port **5000** par défaut, connexion persistante.
- Un message = **un objet JSON UTF-8 sur une ligne**, terminé par `\n`. Taille max d'une ligne : 1 Mo.
- Pas de chiffrement : à n'utiliser que sur un réseau de confiance (TP).

## 2. Connexion

Le client ouvre la connexion TCP, puis envoie `login` ou `register`. En cas d'échec, le serveur répond `error` et
**garde la connexion ouverte** : le client peut retenter (5 essais max, puis fermeture ; 5 min d'inactivité max avant login).

```json
{"cmd":"login","user":"alice","password":"..."}
```

| Réponse | Signification |
|---|---|
| `{"type":"login_ok"}` | authentifié, les commandes sont acceptées |
| `{"type":"error","msg":"identifiants invalides"}` | échec, nouvel essai possible |

### Inscription

```json
{"cmd":"register","user":"alice","password":"...","nom":"Martin","prenom":"Alice","alias":"Ali","email":"alice@r309.fr"}
```

`nom` et `prenom` obligatoires ; `alias` (défaut : prénom) et `email` (défaut : `<user>@r309.fr`) optionnels.
Identifiant : `[A-Za-z0-9_.-]{1,32}`, non déjà pris ; mot de passe : 4 caractères minimum.
Succès : le compte est créé et la session ouverte directement (`login_ok`). Échec : `error` (identifiant déjà pris, etc.), nouvel essai possible.
L'administrateur peut aussi créer un compte en ligne de commande (`python server.py adduser ...`).

L'expéditeur d'un mail est toujours l'utilisateur authentifié de la session (jamais lu dans le message).

## 3. Commandes (client → serveur)

| `cmd` | Champs | Réponse |
|---|---|---|
| `send` | `to` (liste d'identifiants), `subject`, `body` | push `mail` ; `error` si échec |
| `inbox` | — | `inbox` |
| `contacts` | — | `contacts` (liste personnelle) |
| `add_contact` | `contact` (identifiant) | `contacts` à jour ; `error` si inconnu ou soi-même |
| `del_contact` | `contact` (identifiant) | `contacts` à jour |
| `del_mail` | `id` (du mail) | `mail_deleted` ; `error` si inconnu ou si l'utilisateur n'en est ni expéditeur ni destinataire |

```json
{"cmd":"send","to":["bob","carol"],"subject":"Salut","body":"Bonjour !"}
```

## 4. Messages (serveur → client)

| `type` | Champs | Signification |
|---|---|---|
| `login_ok` | — | login accepté |
| `error` | `msg` | échec, message lisible |
| `inbox` | `mails` | tous les mails dont l'utilisateur est expéditeur ou destinataire |
| `contacts` | `contacts` | contacts personnels avec statut de connexion |
| `mail` | `mail` | push : nouveau mail (reçu, ou envoyé par soi-même) |
| `mail_deleted` | `id` | le mail est supprimé **pour cet utilisateur** (le client le retire de sa liste) |
| `offline` | — | **local au client** : connexion perdue (non envoyé par le serveur) |

Objet `mail` : `{"id","from","to":[…],"subject","body","date":"YYYY-MM-DD HH:MM:SS"}`
Objet `contact` : `{"user","nom","prenom","alias","email","online":bool,"ip":str|null}`

`online` et `ip` sont déduits par le serveur de la connexion TCP (adresse source du socket) : un contact est
« en ligne » s'il a au moins une session ouverte.

## 5. Règles de comportement

1. **Livraison** : le mail est stocké (`data.json`), puis poussé à chaque destinataire connecté **et** à l'expéditeur.
2. **Hors-ligne** : un destinataire déconnecté récupère ses mails avec `inbox` à sa prochaine connexion.
3. **Doublons** : le client dédoublonne les mails par `id`.
4. **Reply / Reply all / Forward / export** : côté client uniquement.
5. **Erreurs** : ligne JSON invalide ignorée ; commande inconnue ou champ invalide → `error`.
6. **Champs inconnus** : ignorés (extensibilité).
7. **Suppression** : `del_mail` supprime le mail pour l'utilisateur qui la demande ; il disparaît de son `inbox`,
   les autres participants le gardent. Quand l'expéditeur et tous les destinataires l'ont supprimé, le serveur
   l'efface définitivement de `data.json`. Un mail supprimé n'est jamais renvoyé à celui qui l'a supprimé.

## 6. Mise en service

```
python server.py adduser alice Martin Alice     # optionnel : les comptes peuvent aussi se créer depuis le client
python server.py                                # lance le serveur
python client.py                                # un par utilisateur
```
