# Dragon Ball TCG Pre-Order Monitor (Cloud)

Cloud-runnable versie van de Dragon Ball booster box monitor. Draait elke 2 uur via GitHub Actions, ook als je Mac uit staat.

Wat het doet:
- Scrapet 24 NL/EU/UK shops voor Dragon Ball Masters + Fusion World booster boxes
- Priority watchlist: B31 (Masters) en FB11 (Fusion World)
- Diepe voorraad-check (echte add-to-cart knop) voor priority hits
- Telegram alerts: priority hits, nieuwe pre-orders, restocks, prijs-drops
- Dashboard via GitHub Pages

## Setup (eenmalig)

1. **Repo aanmaken op GitHub**
   - Maak een nieuwe repo aan: `dragonball-monitor` (private of public, jouw keuze)
   - Push deze folder naar die repo:
     ```bash
     cd cloud/dragonball-monitor
     git init
     git add .
     git commit -m "Initial commit: Dragon Ball TCG monitor"
     git branch -M main
     git remote add origin git@github.com:<your-username>/dragonball-monitor.git
     git push -u origin main
     ```

2. **Telegram secrets in GitHub instellen**
   - Ga naar `Settings -> Secrets and variables -> Actions -> New repository secret`
   - Voeg toe:
     - `TELEGRAM_BOT_TOKEN` - jouw bot token (zelfde als in lokale .env)
     - `TELEGRAM_CHAT_ID` - jouw chat ID

3. **GitHub Pages aanzetten** (voor dashboard)
   - `Settings -> Pages -> Source: Deploy from a branch`
   - Branch: `main` / folder: `/ (root)`
   - Save. Na 1-2 min is je dashboard live op:
     `https://<your-username>.github.io/dragonball-monitor/`

4. **Workflow permissies controleren**
   - `Settings -> Actions -> General -> Workflow permissions`
   - Kies "Read and write permissions" zodat de bot `data.json` kan committen

5. **Eerste run handmatig triggeren**
   - `Actions -> Dragon Ball TCG Monitor -> Run workflow`
   - Eerste run = baseline (geen alerts), volgende runs sturen alleen alerts bij wijzigingen

## Wat er gebeurt elke 2 uur

1. GH Action checkt 24 shops + nieuws-sources
2. Detecteert nieuwe pre-orders, restocks, prijs-drops
3. Stuurt Telegram alerts
4. Schrijft `data.json` + `state/*.json` terug naar de repo
5. GitHub Pages ververst het dashboard automatisch

## Lokaal updaten

Het canonical script staat in `execution/tcg_preorder_monitor.py` van de IDE-folder. Wanneer je daar wijzigingen maakt:

```bash
cp ../../execution/tcg_preorder_monitor.py monitor.py
git add monitor.py && git commit -m "Sync monitor" && git push
```

Of pas `monitor.py` direct hier aan en sync terug.

## Troubleshooting

- **Geen alerts ontvangen?** Check Actions tab voor logs. Telegram secrets correct ingesteld? Eerste run is altijd stil (baseline).
- **Action faalt op playwright install?** Workflow gebruikt `--with-deps`, zou moeten werken op `ubuntu-latest`. Check de logs.
- **Dashboard leeg?** Eerste run moet eerst data.json maken. Trigger handmatig via Actions tab.
- **Te veel false positives?** Pas `BOOSTER_BOX_KEYWORDS` / `EXCLUDE_KEYWORDS` / `BLOCKED_SERIES_KEYWORDS` aan in monitor.py.

## Reset state

Verwijder de files in `state/` via een commit, of run `python monitor.py --reset` lokaal en push.
