"""
Export d'une discussion (liste de mails) dans un fichier texte ou JSON.
"""

import json

from . import protocole as P


def _ligne_pj(pj):
    return f"{pj['name']} ({P.taille_lisible(pj.get('size', 0))})"


def exporter_texte(mails, chemin, titre):
    """Fichier texte lisible : un bloc par mail, dans l'ordre chronologique."""
    with open(chemin, "w", encoding="utf-8") as fichier:
        fichier.write(f"{titre}\n{'=' * len(titre)}\n")
        fichier.write(f"Exporté le {P.date_lisible(P.maintenant(), True)} - "
                      f"{len(mails)} message(s)\n\n")
        for numero, mail in enumerate(mails, 1):
            fichier.write(f"--- Message {numero}/{len(mails)} " + "-" * 40 + "\n")
            expediteur = mail["from"]
            if mail.get("from_name"):
                expediteur = f"{mail['from_name']} <{mail['from']}>"
            fichier.write(f"De    : {expediteur}\n")
            fichier.write(f"À     : {', '.join(mail['to'])}\n")
            if mail.get("cc"):
                fichier.write(f"Cc    : {', '.join(mail['cc'])}\n")
            if mail.get("via"):
                fichier.write(f"Via   : liste {mail['via']}\n")
            fichier.write(f"Date  : {P.date_lisible(mail['date'], True)}\n")
            fichier.write(f"Objet : {mail['subject']}\n")
            if mail.get("attachments"):
                fichier.write("PJ    : " + ", ".join(_ligne_pj(pj) for pj in mail["attachments"])
                              + "\n")
            fichier.write("\n" + mail["body"].rstrip() + "\n\n")


def exporter_json(mails, chemin, titre):
    """Fichier JSON structuré (réimportable par un autre programme)."""
    document = {
        "title": titre,
        "exported_at": P.maintenant(),
        "count": len(mails),
        "messages": [{
            "from": m["from"], "from_name": m.get("from_name", ""),
            "to": m["to"], "cc": m.get("cc", []), "via": m.get("via"),
            "date": m["date"], "date_text": P.date_lisible(m["date"], True),
            "subject": m["subject"], "body": m["body"],
            "thread_id": m.get("thread_id"), "message_id": m.get("message_id"),
            "in_reply_to": m.get("in_reply_to"),
            "attachments": [{"name": pj["name"], "mime": pj.get("mime"),
                             "size": pj.get("size")} for pj in m.get("attachments", [])],
        } for m in mails],
    }
    with open(chemin, "w", encoding="utf-8") as fichier:
        json.dump(document, fichier, ensure_ascii=False, indent=2)
