# ex-tackle — Architektur-Review & Konzept für ein zentrales Betriebs-UI

**Erstellt:** 2026-06-21 (Neufassung gegen aktualisierten Code)
**Reviewter Stand:** HEAD `3df3e5d` (Unreleased, nach v1.1.1) — enthält den heutigen Commit *„Fix typo in parameter name and add on_retries_exhausted callback"*
**Betrachtete Nutzung:** `absence` (gepinnt auf `3b439f6` / v1.0.1) sowie flottenweiter Scan unter `~/fahrschule`
**Rolle:** Architektur-/Code-Review mit Bewertung + Lösungskonzept

---

## 1. Management Summary

`ex-tackle` ist eine schlanke, gut lesbare AMQP-Consumer-Bibliothek (Tackle-Topologie: `queue` → `delay` → `dead`). Sie läuft nach eigener Aussage seit über 8 Jahren stabil produktiv — das ist glaubwürdig und kein Widerspruch zu den folgenden Befunden. Der Grund: Die Bibliothek ist im **Normalbetrieb mit stabiler Konfiguration** robust. Die meisten Schwachstellen sind **latente Risiken**, die erst bei Lastspitzen, Fehlerstürmen, Konfigurationsänderungen oder Deployments „feuern" — also genau in den seltenen, dafür teuren Momenten.

Die Bibliothek wird **flottenweit** eingesetzt — als Dependency in mindestens 9 Services (`absence`, `ina_backend`, `marketing_service`, `notifier`, `registration-service`, `simulators`, `sna_backend`, `theory`, `theory_feature_fix_…`; `dlm` trägt ebenfalls Tackle-Konfiguration), mit hunderten Consumer-Deklarationen. Damit multiplizieren sich sowohl die Risiken als auch der Nutzen eines spezialisierten Betriebs-Werkzeugs.

Dieses Dokument liefert zwei Teile:

1. **Schwachstellen-/Code-Smell-Analyse** mit Kritikalitätsbewertung (15 Befunde, davon der von dir genannte „Dead-Messages zurückschieben braucht laufenden Executor"-Fall).
2. **Konzept für ein ex-tackle-spezifisches Betriebs-UI** („Cockpit"), das per Router in jedem Phoenix-Service eingebunden wird, Probleme (Dead Queues, verwaiste Queues/Exchanges, Topologie-Drift) sichtbar macht und gezielte Eingriffe erlaubt — inkl. statischer Wireframes.

> **Hinweis zur Neufassung:** Der heutige Commit `3df3e5d` hat (a) den Tippfehler `max_number_of_attemts` → `max_number_of_attempts` behoben und (b) den Callback `on_retries_exhausted/3` ergänzt, der genau einmal beim Routing in die Dead-Queue feuert. Das ist eine sinnvolle Verbesserung (siehe Abschnitt 3 und Befund 14) und in dieser Fassung berücksichtigt.

---

## 2. Analysebasis & Methodik

Gelesen wurden alle Kernmodule: `Tackle`, `Tackle.Consumer`, `Tackle.Consumer.Executor`, `Tackle.Consumer.Topology`, `Tackle.Consumer.State`, `Tackle.Connection`, `Tackle.Channel`, `Tackle.Exchange`, `Tackle.Queue`, `Tackle.DelayedRetry`, `Tackle.Republisher`, `Tackle.DebugHelper` sowie die Integrationstests. Ergänzend wurde die reale Verwendung im Service `absence` und ein flottenweiter Scan ausgewertet. Zeilenreferenzen beziehen sich auf den aktuellen HEAD `3df3e5d`.

Kritikalität wird wie folgt eingestuft:

| Stufe | Bedeutung |
|---|---|
| **Kritisch** | Kann Ausfall, Crash-Loop oder unbemerkten Datenverlust verursachen. |
| **Hoch** | Ernsthaftes Risiko unter Last oder bei Konfig-/Deploy-Änderungen; zeitnah beheben. |
| **Mittel** | Korrektheits-, Wartbarkeits- oder Betriebsrisiko; situativ relevant. |
| **Niedrig** | Code-Smell / Härtung / Komfort; geringe akute Gefahr. |

---

## 3. Beobachtete Nutzung im Feld (Belege)

Diese Beobachtungen untermauern mehrere Befunde und die Motivation fürs UI:

- **Eigener Wrapper nötig.** `absence` kapselt `Tackle.Consumer` in `Absence.EventConsumer` mit der Begründung im Moduldoc: *„Tackle consumer abstraction, so we can log errors in the consumers and parse the message."* Der Wrapper macht `Jason.decode!`, Logging und Metadaten-Extraktion — genau das, was die Bibliothek selbst nicht anbietet (siehe Befund 13). Es existiert sogar ein `handle_message/2`, das Tackle **nie aufruft** (der Executor ruft nur `handle_message/1`).
- **`on_error/5`-Off-by-one-Workaround — jetzt überflüssig.** Der `absence`-Wrapper nutzt einen Guard `current_retry >= max_number_of_attempts - 1`, um „letzter Versuch" zu erkennen (mit erklärendem Kommentar, weil die Zähl-Semantik verwirrt). Genau dafür gibt es seit Commit `3df3e5d` jetzt `on_retries_exhausted/3` — der Wrapper kann darauf umgestellt werden und den fehleranfälligen Vergleich loswerden.
- **Publisher-Connection wird wiederverwendet — aber nur, weil `absence` die undokumentierte Option kennt.** `Shared.RabbitMQEventPublisher` setzt `publisher_connection_name: "Absence Publisher"`, Consumer nutzen `connection_id: "Absence Consumers"`. Die Option ist in der README nicht dokumentiert; ohne sie öffnet jeder `Tackle.publish`-Aufruf eine neue Verbindung (Befund 4).
- **`exchange_type: :topic` wird in praktisch jedem Service manuell gesetzt** (10 Services setzen es aktiv; in ex-tackle selbst steht es nur als auskommentierter Default). Funktioniert heute, weil alle konsistent sind — aber der Bibliotheks-Default ist `:direct`, und die Einstellung ist global + zur Compile-Zeit fixiert (Befund 6). Ein einziger Service, der die Zeile vergisst, erzeugt `:direct`-Exchanges und damit Broker-Konflikte.
- **Versions-Drift.** Über die Flotte laufen mindestens **drei verschiedene** ex-tackle-Stände gleichzeitig, alle via `branch: main` + Lockfile gepinnt: die meisten auf `3b439f6` (v1.0.1), `notifier` auf `a1932f8` (v1.1.1), `sna_backend` auf einem abweichenden Commit (`8d4e85c3`). Eine fleet-weit genutzte Infrastruktur-Lib auf einem beweglichen Branch zu pinnen ist ein Wartungsrisiko (Befund 15).

---

## 4. Schwachstellen & Code-Smells

### 4.1 Übersicht

| Nr | Befund | Kategorie | Kritikalität |
|---|---|---|---|
| 1 | Topologie-Redeklaration mit abweichenden Argumenten → `PRECONDITION_FAILED` → Crash-/Restart-Schleife | Robustheit | **Hoch** (eskaliert zu Kritisch bei Konfig-Deploys) |
| 2 | Republish verliert Header & Message-Properties | Datenintegrität | **Hoch** |
| 3 | Dead-Queue ohne Dead-Letter-Exchange + TTL → stiller Datenverlust | Datenverlust | **Hoch** |
| 4 | Neue Connection (+Exchange-Deklaration) pro Publish/Retry | Performance/Ressourcen | **Hoch** |
| 5 | Delay-Queue-Name an `retry_delay` gekoppelt → verwaiste Queues bei Konfig-Änderung | Betrieb | **Hoch** |
| 6 | `exchange_type` global & zur Compile-Zeit fixiert | Konfiguration | **Hoch** |
| 7 | Pro-Message-`spawn` ohne Timeout, Backpressure oder Supervision | Robustheit | **Mittel–Hoch** |
| 8 | `retry_dead_messages` erfordert laufenden Executor (Control-Plane an Data-Plane gekoppelt) | Architektur | **Mittel** |
| 9 | `republish` ist Fire-and-forget-`cast` ohne Ergebnis/Fehlerbehandlung | Betrieb | **Mittel** |
| 10 | `nack(requeue:false)` nach manuellem Republish ohne Publisher-Confirms → Duplikat-/Verlust-Fenster | Datenintegrität | **Mittel** |
| 11 | `struct/2` umgeht `@enforce_keys`; keine Options-Validierung → stille Fehlkonfiguration | Robustheit | **Mittel** |
| 12 | `error_reason`-Pattern-Match im `Task.start`-Block → Fehler-Callbacks werden bei untypischem Exit still übersprungen | Beobachtbarkeit | **Niedrig–Mittel** |
| 13 | `handle_message/1` ohne Metadaten/Headers; Payload immer String, kein Content-Type | API-Design | **Mittel** |
| 14 | `on_error`/`on_retries_exhausted` laufen in unüberwachtem `Task` — Ausnahmen verschluckt, Reihenfolgen-Risiko | Beobachtbarkeit | **Mittel** |
| 15 | Sammelposten Härtung: unsichere Verbindung in prod nur geloggt; Connection-Agent-Engpass; kein Reconnect-Backoff; keine Telemetrie; Versions-Drift | Härtung/Wartung | **Niedrig–Mittel** |

### 4.2 Details

**1. Topologie-Redeklaration mit abweichenden Argumenten → Crash-/Restart-Schleife — Hoch**
`Topology.setup!/2` deklariert Queues/Exchanges bei jedem Start mit festen Argumenten (`x-message-ttl`, DLX, `durable`). Existiert eine Queue bereits mit **anderen** Argumenten (z. B. nach Änderung von `retry_delay`/`dead_message_ttl` oder Typwechsel eines Exchanges), antwortet der Broker mit `406 PRECONDITION_FAILED`. `Topology.setup!` wird **innerhalb** des `with`-Blocks aufgerufen, nachdem Verbindung/Channel geöffnet sind (`executor.ex:57`) — der Fehler wird daher **nicht** vom `else`-Zweig abgefangen, sondern lässt `{:ok, _} = AMQP.Queue.declare(...)` (`queue.ex:13`) als MatchError fliegen. Der Executor stürzt ab, der Supervisor startet ihn neu, der nächste Versuch scheitert identisch → **Crash-/Restart-Schleife bis zur Supervisor-Eskalation** (`max_restarts`), die größere Teile des Aufsichtsbaums mitreißen kann. Es gibt keine Erkennung „existiert, aber divergent" und keine Migration. *Empfehlung:* Argumente passiv prüfen (`Queue.declare` `passive: true`) bzw. Topologie-Versionierung/Migration; bei Divergenz klare, einmalige Fehlermeldung statt stiller Schleife.

**2. Republish verliert Header & Properties — Hoch**
`Republisher.republish_one_message/4` liest via `AMQP.Basic.get` nur den **Payload-Body** und published ihn mit fest verdrahtetem `persistent: true` neu (`republisher.ex:19-21`). Alle ursprünglichen Header und Properties gehen verloren: `retry_count`, `content_type`, `correlation_id`, `message_id`, `timestamp` etc. Bei `absence` tragen Events fachliche Metadaten (`causation_id`, `correlation_id`) — diese können bei einem Rescue aus der Dead-Queue verloren gehen oder Folgeverarbeitung verfälschen. *Empfehlung:* `meta` aus `Basic.get` übernehmen und Properties/Header beim Republish erhalten.

**3. Dead-Queue ohne Dead-Letter-Exchange + TTL → stiller Datenverlust — Hoch**
Die Dead-Queue wird nur mit `x-message-ttl` angelegt, **ohne** `x-dead-letter-exchange` (`queue.ex:35-45`). Default-TTL ist ~1 Jahr (`topology.ex:3`). Nach Ablauf werden Dead-Messages vom Broker **endgültig verworfen** — ohne Ziel, ohne Alarm. Dead-Messages sind per Definition die, die man am wenigsten verlieren will. Der neue `on_retries_exhausted/3`-Callback eignet sich gut zum **Alarmieren** beim Eintreffen in der Dead-Queue, ändert aber nichts am stillen Ablaufen danach. *Empfehlung:* Entweder kein TTL auf der Dead-Queue, oder ein „final graveyard"-DLX, in den abgelaufene Dead-Messages fallen, plus Monitoring (das UI deckt das ab).

**4. Neue Connection (+Exchange-Deklaration) pro Publish/Retry — Hoch**
`Tackle.publish/2` und `DelayedRetry.publish/4` laufen über `Tackle.execute(_, :default, _)`, das pro Aufruf eine **komplett neue AMQP-Verbindung** öffnet und wieder schließt (`tackle.ex:40-50`, `delayed_retry.ex:10-14`); zusätzlich wird bei jedem Publish der Exchange neu deklariert (`tackle.ex:28-31`). Pro Nachricht entstehen so TCP-/AMQP-/ggf. TLS-Handshakes — teuer unter Last und bei Fehlerstürmen (jeder fehlgeschlagene Consume erzeugt einen Retry-Publish mit eigener Verbindung). `absence` mildert das für Publishes über die (undokumentierte) Option `publisher_connection_name`; der **Retry-Pfad im Executor nutzt aber weiterhin `:default`** und damit pro Retry eine neue Verbindung. *Empfehlung:* Langlebige Publisher-/Retry-Connection bzw. -Channel-Pool; `publisher_connection_name` dokumentieren und als Default vorsehen; Exchange-Deklaration nicht bei jedem Publish.

**5. Delay-Queue-Name an `retry_delay` gekoppelt → verwaiste Queues — Hoch**
Der Delay-Queue-Name enthält den Delay-Wert: `"#{queue}.delay.#{retry_delay}"` (`topology.ex:104`). Ändert man `retry_delay`, entsteht beim nächsten Start eine **neue** Delay-Queue; die alte bleibt zurück — gebunden, ggf. mit Nachrichten, die nach altem Delay weiterlaufen. Genau diese „verwaisten Queues" willst du im UI sehen. Zusätzlich wird die Delay-Queue **auch bei `retry_limit: 0` angelegt** (FIXME in `topology.ex:53`), obwohl sie nie genutzt wird. *Empfehlung:* Delay über Header/`x-delay` statt über den Queue-Namen steuern; Altbestände erkennen/migrieren (UI).

**6. `exchange_type` global & zur Compile-Zeit fixiert — Hoch**
`@default_exchange_type Application.compile_env(:tackle, :exchange_type, :direct)` (`exchange.ex:7`, inkl. FIXME) ist **global** und **zur Compile-Zeit** gebunden. Folgen: (a) Änderung erfordert Neukompilieren der Dependency; (b) keine Wahl je Consumer/Publisher; (c) Default `:direct` weicht von der gelebten Praxis (`:topic`) ab — vergisst ein Service die Konfig-Zeile, deklariert er denselben Exchange als `:direct` und löst beim Mitnutzer `PRECONDITION_FAILED` aus. Heute sind alle Services konsistent `:topic`, aber die Korrektheit hängt an einer leicht vergessbaren globalen Zeile. *Empfehlung:* Exchange-Typ als reguläre (Laufzeit-)Option pro Topologie.

**7. Pro-Message-`spawn` ohne Timeout, Backpressure oder Supervision — Mittel–Hoch**
Pro eingehender Nachricht wird ein ungebundener, unüberwachter Prozess gestartet (`spawn` in `executor.ex:136`), der wiederum `spawn_link` nutzt und in einem `receive` **ohne Timeout** blockiert (`executor.ex:180-207`). Hängt `handle_message` (z. B. externer Call ohne Timeout), bleibt die Nachricht **für immer** unbestätigt und der Prozess leakt. Bei `prefetch_count > 1` entstehen beliebig viele solcher Prozesse ohne Begrenzung. *Empfehlung:* `Task.Supervisor` mit `Task.async`/`yield` + konfigurierbarem Timeout; gebundene Nebenläufigkeit.

**8. `retry_dead_messages` erfordert laufenden Executor — Mittel** *(dein Beispiel)*
`MyConsumer.retry_dead_messages/1` → `Executor.republish_dead_messages/2` → `GenServer.cast(name, …)` (`consumer.ex:78-81`, FIXME `consumer.ex:79` „Muss nicht zum Executor gehen…"). Das eigentliche Zurückschieben (`Republisher.republish/5`, vom Executor via `handle_cast` aufgerufen, `executor.ex:289`) braucht aber nur **Topologie + URL** — keinen laufenden Consumer. Konsequenz: Man muss den Executor-Prozess laufen lassen, um Dead-Messages zurückzuschieben, obwohl reine Topologie genügt; und das Republish läuft **im** Consumer-Prozess (blockiert Konsum). *Empfehlung:* Republish/Inspect als reine, topologie-basierte Funktionen (Control-Plane) lösen — Fundament für das UI (siehe 5.2).

**9. `republish` ist Fire-and-forget ohne Ergebnis — Mittel**
`GenServer.cast` liefert kein Resultat (`executor.ex:7-9`, `executor.ex:280-290`). Der Aufrufer erfährt nicht, **wie viele** Nachrichten tatsächlich verschoben wurden oder ob ein Fehler auftrat. Sind weniger Nachrichten in der Dead-Queue als `how_many`, passiert für den Rest still nichts. *Empfehlung:* synchrones `call` mit Rückgabe `{moved, errors}`.

**10. `nack(requeue:false)` nach manuellem Republish ohne Confirms — Mittel**
Im Fehlerpfad wird erst manuell in Delay-/Dead-Queue published und **danach** das Original `nack`-t (`executor.ex:130-134`, `executor.ex:262-285`). Beides ist nicht atomar und nutzt **keine Publisher-Confirms** (zudem über separate Verbindung). Stirbt der Prozess dazwischen, bleibt das Original unbestätigt → Redelivery → **Duplikat** (eine Kopie in Delay-Queue + erneute Zustellung). Ist der Ziel-Publish nicht routbar, wird er trotzdem als erfolgreich angesehen → **Verlust**. *Empfehlung:* Publisher-Confirms; Reihenfolge/Idempotenz absichern.

**11. `struct/2` umgeht `@enforce_keys`; keine Options-Validierung — Mittel**
`State.configure!` baut den State mit `struct(__MODULE__, options)` (`state.ex:22-26`). `struct/2` **ignoriert** `@enforce_keys` und unbekannte Keys still. Tippfehler wie `prefetch_count`/`pre_fetch_count` oder `retry_limit` werden kommentarlos verworfen → der Consumer läuft mit Defaults statt der gewünschten Konfiguration; fehlende Pflichtwerte fliegen erst später als kryptischer Folgefehler auf. *Empfehlung:* `struct!/2` + explizite Options-Validierung mit klaren Fehlern (z. B. `NimbleOptions`).

**12. `error_reason`-Pattern-Match im `Task.start`-Block → Fehler-Callbacks still übersprungen — Niedrig–Mittel**
In `retry/4` wird hart destrukturiert: `{may_be_erlang_error, stacktrace} = error_reason` (`executor.ex:241`). Diese Zeile steht **innerhalb** des `Task.start`-Blocks (`executor.ex:238`). Beendet sich der Handler-Prozess mit einem Nicht-2-Tupel-Reason (z. B. `exit(:foo)`), schlägt der Match fehl → **nur der unverlinkte Task stürzt ab** (als Crash-Log sichtbar). Der Executor läuft weiter und die Nachricht wird korrekt in Delay/Dead geroutet — aber `on_error/5` **und** `on_retries_exhausted/3` werden für diesen Fall **stillschweigend übersprungen** (Fehler-Reporting verloren). *Empfehlung:* defensiv matchen (`case`/Fallback) statt strikter Destrukturierung; Callback-Aufruf vom Match entkoppeln.

**13. `handle_message/1` ohne Metadaten; Payload immer String — Mittel**
Der Executor ruft ausschließlich `handle_message(payload)` mit dem rohen String (`executor.ex:123-128`). Routing-Key, Exchange, Header, `redelivered`, Properties und Content-Type sind im Handler **nicht** verfügbar. Genau deshalb baut `absence` einen Wrapper (JSON-Decode, Metadaten) und ein nie aufgerufenes `handle_message/2`. *Empfehlung:* optionales `handle_message/2` mit Metadaten offiziell unterstützen; Content-Type-abhängige (De-)Serialisierung anbieten.

**14. `on_error`/`on_retries_exhausted` in unüberwachtem `Task` — Mittel**
Beide Fehler-Callbacks werden im selben `Task.start`-Block „fire-and-forget" aufgerufen (`executor.ex:237-261`). Probleme: (a) Wirft ein Callback, stirbt nur ein **unverlinkter** Task — das Reporting scheitert **unbemerkt** (nur Crash-Log). (b) `on_error` und `on_retries_exhausted` laufen **sequenziell im selben Task**: Wirft `on_error`, wird `on_retries_exhausted` **nie** erreicht — ausgerechnet beim finalen Fehler, den man melden will. (c) Der Callback-Pfad läuft asynchron/ungeordnet zur Retry-Publikation. Der neue `on_retries_exhausted/3` ist fachlich die richtige Ergänzung (siehe Abschnitt 3), erbt aber dieselbe Ausführungs-Schwäche. *Empfehlung:* Callbacks überwacht ausführen, Ausnahmen je Callback fangen und mindestens loggen; `on_retries_exhausted` unabhängig von `on_error` aufrufen.

**15. Sammelposten Härtung — Niedrig–Mittel**
- **Unsichere Verbindung in prod** wird nur **geloggt**, nicht verweigert (`connection.ex:121-126`) — `amqp://` in Produktion sollte hart abgelehnt werden können.
- **Connection-Agent als Engpass:** alle Verbindungsaufbauten serialisieren über `Agent.get_and_update` (`connection.ex:40-75`); gecachte Verbindungen werden nicht via Monitor überwacht, sondern nur lazy per `Process.alive?` geprüft; `reset/0` ruft `Agent.update` redundant in jeder Iteration (`connection.ex:81-89`).
- **Kein Reconnect-Backoff/Jitter:** festes `reconnect_interval: 1_000` (`state.ex:17`) → Thundering-Herd bei Broker-Ausfall über viele Consumer.
- **Keine Telemetrie/Metriken** in der Lib (kein `:telemetry`) → Beobachtbarkeit muss komplett außen herum gebaut werden (relevant fürs UI).
- **Versions-Drift** (siehe Abschnitt 3): drei ex-tackle-Stände parallel, via `branch: main` gepinnt.

---

## 5. Konzept: „ex-tackle Cockpit" — zentrales Betriebs-UI

### 5.1 Motivation

Die generische LavinMQ-/RabbitMQ-Oberfläche zeigt Queues, aber **nicht** die ex-tackle-Semantik: Sie weiß nichts von der `queue → delay → dead`-Topologie, kennt die Zugehörigkeit Queue↔Consumer-Modul nicht und bietet kein komfortables, properties-erhaltendes Zurückschieben einzelner Dead-Messages. Genau diese Lücken adressiert das Cockpit:

- **Dead Queues sichtbar machen:** welche Nachrichten mit welchem Payload/Headern liegen wo — und gezielt (einzeln/mehrfach) zurückschieben.
- **Verwaiste Ressourcen finden:** Queues/Exchanges ohne zugehörigen (noch existierenden) Consumer, alte `*.delay.<N>`-Queues nach Konfig-Änderung.
- **Topologie-Drift erkennen:** Broker-Ist weicht von Consumer-Soll ab (würde Redeklaration sprengen — Befund 1).
- **Konfiguration inline einsehen/anpassen** (Retries/Delay) — im Rahmen des technisch Möglichen (siehe 5.6).

### 5.2 Designprinzipien

1. **Control-Plane von Data-Plane trennen.** Alle Inspektions-/Eingriffs-Operationen arbeiten **topologie-basiert über kurzlebige, dedizierte Verbindungen** — unabhängig von laufenden Consumer-Prozessen. Das behebt zugleich Befund 8 und macht das UI auch dann nutzbar, wenn ein Consumer gar nicht läuft.
2. **Router-only-Integration.** Einbindung über **eine** Makro-Zeile im Router (Vorbild: Phoenix LiveDashboard, Oban Web). Keine Pflicht-Eingriffe in Application-/Supervision-Tree für die Read-Sicht; Datenzugriff erfolgt lazy beim Seitenaufruf.
3. **Read-mostly, Eingriffe explizit.** Schreibaktionen (Zurückschieben, Löschen, Konfig) nur mit Bestätigung, optionalem Read-only-Modus und Audit-Log.
4. **Pro Service lauffähig, clusterweite Sicht möglich.** Da alle Services dieselbe Broker-Instanz nutzen, kann jede Cockpit-Instanz das Gesamtbild zeigen und „eigene" (per `service`-Präfix) von „fremden/verwaisten" Ressourcen trennen.

### 5.3 Architektur & Datenquellen

Drei Quellen, zusammengeführt zu einem **Soll-/Ist-Abgleich**:

- **(A) Soll — Topologie-Discovery (im Service):** Ermittlung aller Consumer des Service über die laufende Anwendung (Module, die `Tackle.Consumer.Behaviour` implementieren bzw. `topology/0` exportieren) plus optionale Laufzeit-Registry der gestarteten Consumer. Daraus die erwartete Topologie je Consumer (`queue`, `delay_queue`, `dead_queue`, `message_exchange`, `remote_exchange`, Bindings, `retry_limit`, `retry_delay`, `dead_message_ttl`) — alles bereits aus `Topology` ableitbar.
- **(B) Ist — Management-HTTP-API** des Brokers (Port `15672/api`, exakt das Muster, das schon in `test/support/rabbitmq_api.ex` existiert): Queues, Tiefen, Consumer-Anzahl, Raten, Queue-Argumente, Exchanges, Bindings. **Zerstörungsfreies Peeken** von Dead-Messages inkl. Payload + Properties + Headern über `POST /api/queues/{vhost}/{name}/get` mit `ackmode: ack_requeue_true`.
- **(C) Eingriffe — kurzlebige AMQP-Verbindung** (oder Management-`publish`): properties-erhaltendes Republish, Purge, Löschen verwaister Ressourcen, gezieltes Verschieben einzelner Nachrichten.

Kein zusätzlicher Datenspeicher nötig (Quelle ist Broker + Modul-Introspektion); ein optionales Audit-Log kann in DB/Logger fließen.

### 5.4 Problemerkennung (Detektoren)

| Detektor | Quelle | Adressiert Befund |
|---|---|---|
| Dead-Queue mit `messages > 0` (+ Payload-Inspektion) | A×B | 2, 3 |
| Verwaiste Queue: passt aufs Tackle-Namensschema, aber kein zugehöriges Consumer-Modul (z. B. alte `*.delay.<N>`, Queue gelöschter Consumer) | A×B | 5 |
| Queue ohne aktive Consumer (`consumers = 0`), obwohl Soll-Consumer existiert | A×B | — (Betrieb) |
| Verwaister Exchange (keine Bindings / kein Consumer) | A×B | 5 |
| Topologie-Drift: Broker-Argumente ≠ Soll (`x-message-ttl`, DLX, Typ) → „Redeklaration würde scheitern" | A×B | 1, 6 |
| Retry-/Dead-Raten-Anomalie (Spikes in Delay/Dead) | B | 7 |
| Unsichere Verbindung / fehlendes TLS (optional) | A | 15 |

### 5.5 Screens & Wireframes

**S1 — Übersicht / Health-Dashboard**

```
┌─ ex-tackle Cockpit ───────────────────────────── Service: absence ▼ ─┐
│ [ Übersicht ] [ Dead Queues ] [ Verwaist ] [ Topologie/Config ]  [⟳] │
├──────────────────────────────────────────────────────────────────────┤
│  3 Probleme    2 verwaiste Queues    1 Dead Queue (12 Nachrichten)    │
├────────────────────────┬───────┬────────┬───────┬───────┬────────────┤
│ Consumer               │ Queue │ In-Flt │ Delay │ Dead  │ Status     │
├────────────────────────┼───────┼────────┼───────┼───────┼────────────┤
│ CompanyDefinedConsumer │   0   │   0    │   0   │  12 ! │  degraded  │
│ LocationModifiedConsumer│  3   │   1    │   0   │   0   │  ok        │
│ NewEmployeeHiredConsumer│  0   │   0    │   0   │   0   │  no consumer! │
└────────────────────────┴───────┴────────┴───────┴───────┴────────────┘
```

**S2 — Dead-Queue-Inspector** (Kern-Use-Case: sehen, was mit welchem Payload liegt, und zurückschieben)

```
┌─ Dead Queue: absence.company-defined.dead   (12 Nachrichten) ─────────┐
│ [ Auswahl zurückschieben ] [ Alle zurückschieben ] [ Löschen ]   [⟳] │
├──┬─────────────────────┬───────────────────┬────────┬───────────────┤
│☑ │ Zeitpunkt           │ routing_key       │ retries│ Payload-Preview│
├──┼─────────────────────┼───────────────────┼────────┼───────────────┤
│☑ │ 2026-06-03 14:21:09 │ company-defined   │   3    │ {"id":4711,…}  │
│☐ │ 2026-06-03 14:19:55 │ company-defined   │   3    │ {"id":4712,…}  │
│  │ …                   │                   │        │                │
├──┴─────────────────────┴───────────────────┴────────┴───────────────┤
│ ▼ Detail #4711                                                       │
│   Headers:    retry_count=3   content_type=application/json          │
│   Properties: message_id=…  correlation_id=…  causation_id=…         │
│   Payload:    { "id":4711, "type":"CompanyDefined", "actor":… }      │
│   Ziel:   (•) Original-Exchange (absence.company-defined)            │
│           ( ) Anderer Exchange / Routing-Key …                      │
│   [ Diese Nachricht zurückschieben ]   [ Payload kopieren ]         │
└──────────────────────────────────────────────────────────────────────┘
```

Das Zurückschieben **erhält Header & Properties** (behebt Befund 2) und läuft über eine kurzlebige Verbindung **ohne** laufenden Consumer (behebt Befund 8).

**S3 — Verwaiste Queues & Exchanges**

```
┌─ Verwaiste Ressourcen ───────────────────────────────────────────────┐
│ Erkannt über Soll-/Ist-Abgleich (Consumer-Topologie ↔ Broker)        │
├───────────────────────────────────┬──────────┬──────────┬───────────┤
│ Ressource                         │ Typ      │ Inhalt   │ Grund     │
├───────────────────────────────────┼──────────┼──────────┼───────────┤
│ absence.company-defined.delay.10  │ Queue    │ 0 Msgs   │ retry_delay│
│   (aktiv ist jetzt delay.30)      │          │          │ geändert  │
│ absence.legacy-import             │ Queue    │ 47 Msgs  │ Consumer  │
│                                   │          │          │ entfernt  │
│ absence.legacy-import (exchange)  │ Exchange │ 0 Binds  │ verwaist  │
├───────────────────────────────────┴──────────┴──────────┴───────────┤
│ Aktion:  [ Inhalt sichern/peeken ]   [ Löschen … ] (Doppelbestätigung)│
└──────────────────────────────────────────────────────────────────────┘
```

**S4 — Topologie & Retry-Konfiguration**

```
┌─ Topologie & Retry-Konfig: CompanyDefinedConsumer ───────────────────┐
│ remote_exchange : charger                                            │
│ message_exchange: absence.company-defined                            │
│ queue           : absence.company-defined                            │
│ delay_queue     : absence.company-defined.delay.30                   │
│ dead_queue      : absence.company-defined.dead                       │
├──────────────────────────────────────────────────────────────────────┤
│ retry_limit       [ 5  ]    (Code-seitig → erfordert Consumer-Neustart)│
│ retry_delay (s)   [ 30 ]    ! Broker-Argument → Migration nötig       │
│ dead_message_ttl  [ 1y ]    ! Broker-Argument → Queue-Neuanlage nötig │
├──────────────────────────────────────────────────────────────────────┤
│ Soll-/Ist-Abgleich:  Argumente konsistent                            │
│ [ Änderungen vorbereiten ]  → zeigt Migrationsplan VOR Ausführung    │
└──────────────────────────────────────────────────────────────────────┘
```

### 5.6 Inline-Konfiguration: Machbarkeit & Grenzen (ehrlich)

Wichtig: „Inline anpassen" ist nicht für alle Werte gleich einfach, weil manche Werte **Broker-Queue-Argumente** sind (in RabbitMQ/LavinMQ nach Anlage unveränderlich):

- **`retry_limit`** ist **kein** Broker-Argument, sondern wird im Code gegen `topology.retry_limit` geprüft. Eine Änderung erfordert **keine** Queue-Migration — aber der Wert ist heute aus den Consumer-Optionen einkompiliert. Live änderbar wäre er nur über (a) Consumer-Neustart mit neuen Optionen oder (b) eine Bibliotheks-Erweiterung, die den Wert zur Laufzeit aus einer veränderbaren Quelle (Config/DB) liest.
- **`retry_delay`** steckt im `x-message-ttl` der Delay-Queue **und** im Queue-Namen (Befund 5). Änderung ⇒ **neue** Queue + Migration der Inhalte + Aufräumen der alten.
- **`dead_message_ttl`** ist `x-message-ttl` der Dead-Queue ⇒ Änderung nur per Neuanlage.

Konsequenz fürs UI: Es bietet **kein** stilles Inline-Schreiben, sondern einen **kontrollierten Migrationsplan** („neue Topologie anlegen → Nachrichten umziehen → alte entfernen") mit Vorschau und Bestätigung. Für echtes, persistentes Inline-Editieren empfiehlt sich als Vorarbeit eine **dynamische, laufzeit-/persistenzfähige Topologie** in ex-tackle (Werte aus Config/DB statt aus Modul-Optionen). Ohne diese Erweiterung bleibt S4 auf „anzeigen + Migration anstoßen + ggf. Neustart markieren" beschränkt — was bereits einen großen Teil des heutigen Schmerzes nimmt.

### 5.7 Multi-Service / zentrale Nutzung

- **Variante A (empfohlen):** In jedem Service identisch per Router gemountet. Da alle dieselbe Broker-Instanz nutzen, sieht jede Instanz clusterweit alle Tackle-Queues und filtert „eigene" (Präfix aus der `service`-Option) vs. „fremde/verwaiste". Ein „Alle Services"-Tab zeigt das Gesamtbild. Minimaler Infrastrukturaufwand, erfüllt den Router-only-Wunsch.
- **Variante B:** dedizierter zentraler Aggregator-Service, der ausschließlich die Management-API abfragt; die Pro-Service-Mounts zeigen nur Eigenes. Mehr Trennung, mehr Betrieb.

Empfehlung: **A**, weil sie ohne zusätzliche Dienste auskommt und die Service-Identität sauber aus der Topologie ableitbar ist.

### 5.8 Router-Integration & Technologie

- **Technologie:** Phoenix **LiveView** (Echtzeit-Updates, mountbar, etabliertes Muster wie Phoenix LiveDashboard / Oban Web / Kaffy).
- **Integration:** genau **eine** Zeile im Router innerhalb eines `scope`, analog `live_dashboard "/dashboard"` — z. B. ein Makro `tackle_dashboard "/ex-tackle"`. Authentifizierung/Autorisierung über die **vorhandene Router-Pipeline** (Basic-Auth, SSO, IP-Allowlist). Keine weiteren Pflicht-Änderungen am Service.
- **Verteilung als Bibliothek:** eigenes Hex-/Git-Paket (z. B. `tackle_web`) mit Abhängigkeit auf `tackle`; so bleibt der Kern schlank und das UI optional einbindbar.

### 5.9 Sicherheit & Betrieb

- **Auth Pflicht** für alle Eingriffe; **Read-only-Modus** per Konfiguration.
- **Bestätigungen + Audit-Log** für Republish/Purge/Delete (wer, wann, wie viele, wohin).
- **Schreibaktionen über kurzlebige, dedizierte Verbindung** (nicht über Consumer-Prozesse) — konsistent zur Control-/Data-Plane-Trennung.
- **Schonung der Management-API:** Caching/Throttling der Abfragen; gerade bei vielen Queues (hunderte fleet-weit) relevant.
- **Secrets:** Broker-Zugangsdaten nur serverseitig; nie ins Frontend.

### 5.10 Umsetzungs-Roadmap

- **Phase 0 — Bibliotheks-Vorarbeit (entkoppelt vom UI, bringt sofort Wert):**
  topologie-basierte Inspect-/Republish-API ohne Executor (Befund 8); properties-erhaltendes Republish (Befund 2); Consumer-Discovery/Registry; optional `:telemetry`-Events (Befund 15).
- **Phase 1 — Read-only-Cockpit:** S1-Übersicht, S2-Inspektion (Peek), S3-Erkennung verwaister Ressourcen. Kein Risiko, sofort nützlich.
- **Phase 2 — Eingriffe:** Zurückschieben (properties-erhaltend), Purge, Löschen verwaister Ressourcen — mit Auth, Bestätigung, Audit.
- **Phase 3 — Konfig & Migration:** S4 inkl. Migrationsplan; clusterweite „Alle Services"-Sicht.

---

## 6. Empfohlene Sofortmaßnahmen (Priorität)

1. **Befund 3 (Dead-TTL/DLX)** und **Befund 1 (Topologie-Drift/Crash-Loop)** zuerst — beide bergen unbemerkten Verlust bzw. Ausfallpotenzial bei Deploys.
2. **Befund 2** (Properties beim Republish erhalten) — bevor das UI das Zurückschieben breit ausrollt.
3. **Befund 8 + 9** als Phase-0-Vorarbeit fürs UI (topologie-basierte, synchrone Republish-/Inspect-API).
4. **Befund 4** (langlebige Publisher-/Retry-Connection) und **Befund 6** (`exchange_type` als Laufzeit-Option) — Performance- bzw. Konfig-Landmine flottenweit.
5. **Befund 15 / Versions-Drift** — ex-tackle auf getaggte Releases statt `branch: main` pinnen und Flotte angleichen.

---

## 7. Annahmen & offene Punkte

- Das Konzept geht davon aus, dass die Management-HTTP-API des Brokers (LavinMQ ist hier weitgehend RabbitMQ-kompatibel) erreichbar und für die Services nutzbar ist — das in `test/support/rabbitmq_api.ex` genutzte Muster bestätigt das. Die genauen Endpunkte/Capabilities für **zerstörungsfreies Peeken** und **Publish mit Properties** sollten gegen die konkrete LavinMQ-Version verifiziert werden.
- Stand der Analyse ist HEAD `3df3e5d`; die meisten produktiven Services laufen auf älteren Ständen (v1.0.1) und kennen `on_retries_exhausted/3` noch nicht. Vor Umsetzung lohnt ein kurzer Abgleich, welche Befunde im jeweils gepinnten Stand bereits anders sind.
- Belege aus realer Nutzung stammen aus `absence` und einem flottenweiten Scan; weitere Services können das Bild verfeinern (z. B. ob ein Service `publisher_connection_name` *nicht* setzt und damit Befund 4 voll trifft).
