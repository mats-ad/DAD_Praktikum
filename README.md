# DAD_Praktikum – Projektübersicht

Ziel ist die prototypische Umsetzung einer **Lambda-Architektur**, die:

- mehrere externe Datenquellen integriert,
- Streaming- und Batch-Verarbeitung kombiniert,
- verteilte Systeme (Kafka, Spark) nutzt,
- und fachliche Analysen ermöglicht.

Der Fokus liegt auf **Architektur, Datenfluss, Nachvollziehbarkeit und Analysefähigkeit** – nicht auf einer produktionsreifen Lösung.

---

### Zielsetzung

Das Ziel des Systems ist die kontinuierliche Erfassung und Analyse von:

- **Aktienmarktdaten** (Preisentwicklung ausgewählter Aktien)
- **Social-Media-Daten** (Diskussionen und Erwähnungen von Aktien auf Reddit)

Dabei sollen sowohl:

- **nahezu echtzeitfähige Auswertungen (Speed Layer)**
- als auch **aggregierte, historisierte Analysen (Batch Layer)**
  unterstützt werden.

Typische Fragestellungen:

- Wie entwickeln sich Aktienpreise aktuell?
- Welche Aktien werden aktuell besonders häufig diskutiert?
- Gibt es zeitliche Zusammenhänge zwischen Social Buzz und Preisen?

---

### Datenquellen

#### Finanzmarktdaten

- Quelle: Yahoo Finance (öffentliche Schnittstelle)
- Datentyp: strukturierte Zeitreihendaten
- Erfasste Attribute:
  - Aktien-Symbol
  - aktueller Preis
  - Ingest-Zeitpunkt
  - Datenquelle

#### Social-Media-Daten

- Quelle: Reddit (öffentliches JSON API, kein Login)
- Subreddits:
  - r/stocks
  - r/investing
  - r/wallstreetbets
- Datentyp: unstrukturierter Text
- Erfasste Attribute:
  - Titel
  - Text
  - Score
  - Anzahl Kommentare
  - Erstellungszeitpunkt
  - erkannte Aktien-Ticker

---

### Ingest Layer

Der Ingest Layer ist verantwortlich für die **Erfassung externer Datenquellen** und deren Übergabe an Kafka.

#### Finanzdaten-Ingest

- periodischer Abruf von Aktienpreisen
- Serialisierung als JSON
- Versand an Kafka Topic `prices_raw`

#### Reddit-Ingest

- Scraping öffentlicher Reddit-Listings
- Verwendung eines expliziten User-Agents
- Deduplizierung über Post-ID
- Versand an Kafka Topic `social_raw`

Der Ingest Layer ist bewusst **einfach und robust** gehalten.

---

### Speed Layer

Die Speed Layer verarbeitet eingehende Datenströme **kontinuierlich** mittels Apache Spark Structured Streaming.

#### Prices Speed Layer

- liest aus Kafka Topic `prices_raw`
- extrahiert Symbol und Preis
- verwirft ungültige Events
- speichert Ergebnisse append-only als Parquet

Ziel: niedrige Latenz für aktuelle Preisdaten.

#### Social Speed Layer

- liest aus Kafka Topic `social_raw`
- führt Textvorverarbeitung durch
- erkennt Aktien-Ticker per Regex
- speichert nur relevante Posts im Data Lake

---

### Speed Layer

Die Speed Layer verarbeitet eingehende Datenströme **kontinuierlich** mittels Apache Spark Structured Streaming.

#### Prices Speed Layer

- liest aus Kafka Topic `prices_raw`
- extrahiert Symbol und Preis
- verwirft ungültige Events
- speichert Ergebnisse append-only als Parquet

Ziel: niedrige Latenz für aktuelle Preisdaten.

#### Social Speed Layer

- liest aus Kafka Topic `social_raw`
- führt Textvorverarbeitung durch
- erkennt Aktien-Ticker per Regex
- speichert nur relevante Posts im Data Lake

---

### Data Lake

Alle verarbeiteten Daten werden im **Parquet-Format** im lokalen Data Lake abgelegt.

Struktur:
```ASCII
ta/
└── lake/
├── prices/
│ ├── speed/
│ └── batch_daily/
└── social/
└── reddit/
├── speed/
└── batch_daily/

D
```ie Parquet-Dateien können direkt mit:

- Pandas
- Spark
- oder Notebooks

analysiert werden.

---

### Orchestrierung

Die Orchestrierung erfolgt über ein zentrales Python-Skript (`main.py`).

Aufgaben:

1. Start der Docker-Infrastruktur
2. Erstellen der Kafka Topics
3. Start der Ingest-Prozesse
4. Start der Speed Layer (Spark Streaming)
5. Periodische Ausführung der Batch Jobs

Die Orchestrierung ist bewusst einfach gehalten und dient der Demonstration des Gesamtflusses.

---

### Ausführung des Projekts

#### Voraussetzungen

- Docker & Docker Compose
- Python ≥ 3.10
- Apache Spark (lokale Installation)
- Java (für Spark)
- Virtuelle Python-Umgebung (empfohlen)

#### Installation der Python-Abhängigkeiten

Virtuelle Umgebung anlegen und aktivieren:

```bash
python -m venv dad_env
source dad_env/bin/activate
```

Abhängigkeiten installieren:

```bash
pip install -r requirements.txt
```

Benötigte Pakete (Auswahl):

- kafka-python
- requests
- pyspark
- pandas
- python-dotenv

## Konfiguration

Eine `.env` Datei im Projektverzeichnis anlegen:

KAFKA_BROKER=localhost:9092

Hinweis: Für Yahoo Finance ist kein API-Key erforderlich.

#### Start der gesamten Pipeline

Die komplette Pipeline wird über das zentrale Skript gestartet:

```bash
python main.py
```

Dieses Skript:

1. startet die Docker-Infrastruktur
2. erstellt die Kafka Topics
3. startet die Ingest-Prozesse
4. startet die Speed Layer (Spark Streaming)
5. führt periodisch Batch Jobs aus

Die Pipeline läuft kontinuierlich, bis sie manuell beendet wird.

#### Beenden der Pipeline

Die Pipeline kann mit `Ctrl + C` beendet werden.  
Alle gestarteten Prozesse werden dabei sauber terminiert.

Container Runterfahren

```bash
docker compose down
```

#### Daten prüfen

Die erzeugten Daten können direkt aus dem Data Lake gelesen werden, z. B. mit Pandas:

```bash
python - <<EOF
import pandas as pd
print(pd.read_parquet("data/lake/social/reddit/batch_daily"))
EOF
```

Analog können Speed- und Batch-Daten für Preise geprüft werden.
