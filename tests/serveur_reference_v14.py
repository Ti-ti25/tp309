"""
Serveur de RÉFÉRENCE minimal du protocole R309 v1.4, écrit uniquement à
partir de PROTOCOLE_TCP.md, sans réutiliser le code du projet.

Il joue le rôle du « serveur d'un autre binôme » pour vérifier que notre
client (messagerie/client_tcp.py) est interopérable. Particularités
volontaires : les identifiants de mails sont des ENTIERS, et les dates au
format "YYYY-MM-DD HH:MM:SS".
"""

import json
import socket
import threading
import time


class ServeurReference:
    def __init__(self, port=0):
        self.comptes = {}            # user -> dict(password, nom, prenom, alias, email)
        self.contacts = {}           # user -> [users]
        self.mails = {}              # id -> mail
        self.supprime_par = {}       # id -> set(users)
        self.sessions = {}           # user -> [conn]
        self.ip = {}
        self.prochain_id = 1
        self.verrou = threading.Lock()
        self.ecoute = socket.socket()
        self.ecoute.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ecoute.bind(("127.0.0.1", port))
        self.ecoute.listen()
        self.port = self.ecoute.getsockname()[1]
        threading.Thread(target=self._accueil, daemon=True).start()

    def _accueil(self):
        while True:
            try:
                conn, adr = self.ecoute.accept()
            except OSError:
                return
            threading.Thread(target=self._client, args=(conn, adr), daemon=True).start()

    @staticmethod
    def _env(conn, msg):
        conn.sendall((json.dumps(msg) + "\n").encode())

    def _client(self, conn, adr):
        user, tampon = None, b""
        try:
            while True:
                donnees = conn.recv(4096)
                if not donnees:
                    break
                tampon += donnees
                while b"\n" in tampon:
                    ligne, tampon = tampon.split(b"\n", 1)
                    try:
                        msg = json.loads(ligne)
                    except ValueError:
                        continue
                    user = self._traiter(conn, adr, user, msg)
        except OSError:
            pass
        finally:
            if user:
                with self.verrou:
                    self.sessions[user].remove(conn)
            conn.close()

    def _contacts(self, user):
        return [dict(user=u, nom=self.comptes[u]["nom"], prenom=self.comptes[u]["prenom"],
                     alias=self.comptes[u]["alias"], email=self.comptes[u]["email"],
                     online=bool(self.sessions.get(u)),
                     ip=self.ip.get(u) if self.sessions.get(u) else None)
                for u in self.contacts.get(user, [])]

    def _traiter(self, conn, adr, user, msg):
        cmd = msg.get("cmd")
        with self.verrou:
            if cmd in ("login", "register"):
                u = msg.get("user")
                if cmd == "register":
                    if u in self.comptes or not msg.get("nom") or not msg.get("prenom"):
                        self._env(conn, {"type": "error", "msg": "inscription refusée"})
                        return user
                    self.comptes[u] = dict(password=msg.get("password"), nom=msg["nom"],
                                           prenom=msg["prenom"],
                                           alias=msg.get("alias") or msg["prenom"],
                                           email=msg.get("email") or f"{u}@r309.fr")
                elif self.comptes.get(u, {}).get("password") != msg.get("password"):
                    self._env(conn, {"type": "error", "msg": "identifiants invalides"})
                    return user
                self.sessions.setdefault(u, []).append(conn)
                self.ip[u] = adr[0]
                self._env(conn, {"type": "login_ok"})
                return u
            if user is None:
                self._env(conn, {"type": "error", "msg": "non authentifié"})
                return user
            if cmd == "send":
                dest = msg.get("to") or []
                if not dest or any(d not in self.comptes for d in dest):
                    self._env(conn, {"type": "error", "msg": "destinataire inconnu"})
                    return user
                mail = {"id": self.prochain_id, "from": user, "to": dest,
                        "subject": msg.get("subject", ""), "body": msg.get("body", ""),
                        "date": time.strftime("%Y-%m-%d %H:%M:%S")}
                self.prochain_id += 1
                self.mails[mail["id"]] = mail
                for u in set(dest) | {user}:
                    for c in self.sessions.get(u, []):
                        self._env(c, {"type": "mail", "mail": mail})
            elif cmd == "inbox":
                mails = [m for i, m in self.mails.items()
                         if (m["from"] == user or user in m["to"])
                         and user not in self.supprime_par.get(i, set())]
                self._env(conn, {"type": "inbox", "mails": mails})
            elif cmd == "contacts":
                self._env(conn, {"type": "contacts", "contacts": self._contacts(user)})
            elif cmd == "add_contact":
                c = msg.get("contact")
                if c not in self.comptes or c == user:
                    self._env(conn, {"type": "error", "msg": "contact invalide"})
                    return user
                if c not in self.contacts.setdefault(user, []):
                    self.contacts[user].append(c)
                self._env(conn, {"type": "contacts", "contacts": self._contacts(user)})
            elif cmd == "del_contact":
                if msg.get("contact") in self.contacts.get(user, []):
                    self.contacts[user].remove(msg["contact"])
                self._env(conn, {"type": "contacts", "contacts": self._contacts(user)})
            elif cmd == "del_mail":
                m = self.mails.get(msg.get("id"))
                if m is None or (m["from"] != user and user not in m["to"]):
                    self._env(conn, {"type": "error", "msg": "mail inconnu"})
                    return user
                self.supprime_par.setdefault(m["id"], set()).add(user)
                self._env(conn, {"type": "mail_deleted", "id": m["id"]})
            else:
                self._env(conn, {"type": "error", "msg": "commande inconnue"})
        return user

    def arreter(self):
        self.ecoute.close()
