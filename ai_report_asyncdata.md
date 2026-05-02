# AI Report

Při vypracování tohoto úkolu jsem jako hlavního pomocníka při programování a debuggingu využil Google Gemini (model Pro). Celou aplikaci pro zpracování úloh – Image Worker (`worker.py`) – jsem vytvořil ve spolupráci s Gemini, a to včetně implementace matematických operací nad NumPy maticemi a asynchronního napojení na Message Broker.

Kromě samotného Workera mi AI pomohla s těmito body:
* **Integrace projektů:** Sloučení původního Storage projektu a nového Message Brokera do jedné funkční aplikace.
* **Asynchronní databáze:** Přepis synchronních databázových operací na plně asynchronní pomocí `AsyncSession`.
* **Debugging:** Odhalení a oprava chyb při ukládání obrázků (doplnění chybějících přípon pro knihovnu Pillow) a vysvětlení chování brokera při práci s prázdnou databází.
* **Testovací UI:** Vygenerování jednoduchého uživatelského rozhraní v HTML/JS pro pohodlnější vizualizaci celého procesu (vytvoření bucketu, nahrání obrázku, zpracování přes brokera a ověření účtování).