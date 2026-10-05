#!/bin/sh
# Lance le broker Mosquitto avec la configuration du projet.
#   ./lancer_broker.sh            -> configuration simple (anonyme, sans ACL)
#   ./lancer_broker.sh securise   -> ACL + compte "serveur" (mot de passe : $MDP_SERVEUR, défaut "serveur")
set -e
REP="$(cd "$(dirname "$0")/mosquitto" && pwd)"

if [ "$1" = "securise" ]; then
    RUN="${TMPDIR:-/tmp}/messagerie-mosquitto"
    mkdir -p "$RUN"
    # Fichier de mots de passe (haché par mosquitto_passwd)
    rm -f "$RUN/passwd"
    mosquitto_passwd -c -b "$RUN/passwd" serveur "${MDP_SERVEUR:-serveur}"
    cp "$REP/acl.conf" "$RUN/acl.conf"
    sed "s#@REP@#$RUN#g" "$REP/mosquitto_securise.conf" > "$RUN/mosquitto.conf"
    if [ "$(id -u)" = "0" ]; then
        # En root, mosquitto passe sous l'utilisateur "mosquitto" : il doit pouvoir lire ces fichiers.
        chown -R mosquitto "$RUN" 2>/dev/null || echo "user root" >> "$RUN/mosquitto.conf"
    fi
    chmod 700 "$RUN"; chmod 600 "$RUN/passwd" "$RUN/acl.conf"
    exec mosquitto -c "$RUN/mosquitto.conf" -v
else
    exec mosquitto -c "$REP/mosquitto.conf" -v
fi
