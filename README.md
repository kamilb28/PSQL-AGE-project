# Uruchamianie

## 1. Pobranie obrazu dockera'

`docker pull apache/age`

`docker run --name age-container -e POSTGRES_PASSWORD=$haslo -p 5432:5432 -d apache/age`

w **$haslo** należy wpisać hasło do bazy psql, ale zalecane jest `root` bo takie jest stosowane w skryptach

## 2. Pobranie bibliotek

Na systemie powinien być zainstalowany python, zalecamy również stowrzenie wirtualnego środowiska:

`python3 -m venv venv`

`source venv/bin/activate`

Pobranie bibliotek:

`pip install -r requirements.txt`

## 3. Import danych 

Należy w głównym folderze mieć dwa pliki: **popularity_iw/csv.gz**, **taxonomy_iw/csv.gz**

Uruchomić skrypt:

`python import_v3.py`

## 4. Narzędzie

Należy uruchomić:

`python dbctl.py $number_zadania $arg1 $arg2`

# Przydatne komendy

`docker exec -it age psql -U postgres` - wejście do bazy

Uruchomienie AGE w bazie:

`LOAD 'age';`

`SET search_path = ag_catalog, "$user", public;`

Usunięcie grafu:

`SELECT * FROM drop_graph('iw_graph', true);`

Dodanie RAMu do kontenera:

`docker update --memory=2g --memory-swap=3g age-container`