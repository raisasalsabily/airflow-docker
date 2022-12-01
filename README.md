# airflow-docker
 
 ## Cara Instalasi
 
- clone di sebuah folder
- aktifkan docker desktop
- run command "docker-compose up airflow-init" di terminal pada direktori folder utama
- tunggu sampah semua image dan server pada kontainer menyala. untuk cek, dapat run command "docker ps" pada terminla folder utama
- jika statusnya sudah healthy, lanjut ke step berikut. jika belum, maka run "docker-compose up" pada terminal folder utama
- untuk akses dashboard airflow, akses localhost:8080
- untuk akses dashboard pgAdmin docker, akses localhost:15432
- email pgAdmin: duabelas@mail.com, pass: duabelas
- run twiiter_dag pada dashboard airflow
