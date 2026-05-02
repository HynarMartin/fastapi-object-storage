# Výsledky zátěžových testů (Benchmark)

## 1. Konfigurace testovacího prostředí
Testy proběhly na následujícím hardwaru a softwaru:
* **Notebook:** Lenovo LOQ 15IRH8
* **Procesor (CPU):** Intel(R) Core(TM) i5-13500H (13. generace, 2.60 GHz)
* **Operační paměť (RAM):** 16.0 GB
* **Operační systém:** Windows Subsystem for Linux (WSL) - distribuce Ubuntu

*Poznámka k metodice:* Abychom otestovali skutečnou propustnost asynchronních WebSocketů a změřili čistý rozdíl mezi kompresními formáty, byl pro účely benchmarku implementován parametr `BENCHMARK_MODE`. Ten dočasně obchází ukládání zpráv do SQLite databáze. Při zapnuté databázi se rychlost pohybovala na úrovni ~33 msg/s, jelikož se SQLite při extrémním množství souběžných I/O zápisů (Publish + ACK z vícero klientů současně) zamykala a tvořila úzké hrdlo systému. Níže uvedená měření tak reflektují čistou propustnost přes síť a RAM (1000 zpráv na každého z 5 Publisherů, celkem 5000 zpráv).

## 2. Naměřená propustnost (Throughput)
Test spouštěl asynchronně 5 Publisherů a 5 Subscriberů.

* **Formát JSON (textový):** 2423.91 msg/s (5000 zpráv za 2.06s)
* **Formát MessagePack (binární):** 2551.23 msg/s (5000 zpráv za 1.96s)

**Zhodnocení:**
Při měření čisté propustnosti se ukázalo, že binární formát MessagePack je mírně rychlejší na zpracování i při malých zprávách. Jeho největší výhoda v reálném cloudovém nasazení (např. AWS) by se však projevila ve velikosti přenášených dat (payload bytes). Binární komprese masivně snižuje objem dat na síti, což přímo šetří finanční náklady za síťový přenos (egress fee) mezi mikroslužbami. Použití binárního formátu se tedy u vysoce vytížených brokerů rozhodně vyplatí.