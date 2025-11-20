#!/bin/bash

# Script d'initialisation du cluster Hadoop
echo "Début de l'initialisation du cluster Hadoop..."

# Créer le répertoire pour les scripts
mkdir -p scripts

# Créer le script d'initialisation Hadoop pour le master
cat > scripts/init-and-start-hadoop.sh << 'EOF'
#!/bin/bash

echo "Configuration du Hadoop Master..."

# Tester la connectivité avec les slaves
echo "Test de connexion aux slaves..."
ping -c 2 hadoop-slave1
ping -c 2 hadoop-slave2

# Configurer la liste des workers
echo "Configuration des workers Hadoop..."
echo "hadoop-slave1" > /usr/local/hadoop/etc/hadoop/workers
echo "hadoop-slave2" >> /usr/local/hadoop/etc/hadoop/workers

echo "Liste des workers:"
cat /usr/local/hadoop/etc/hadoop/workers

# Formater HDFS si premier démarrage
if [ ! -f /hadoop/data/formatted ]; then
    echo "Formatage de HDFS..."
    hdfs namenode -format -force -nonInteractive
    if [ $? -eq 0 ]; then
        touch /hadoop/data/formatted
        echo "HDFS formaté avec succès"
    else
        echo "Erreur lors du formatage HDFS"
        exit 1
    fi
else
    echo "HDFS déjà formaté"
fi

# Démarrer les services Hadoop
echo "Démarrage des services Hadoop..."

# Démarrer HDFS
echo "Démarrage de HDFS..."
start-dfs.sh
sleep 10

# Démarrer YARN
echo "Démarrage de YARN..."
start-yarn.sh
sleep 5

# Démarrer History Server
echo "Démarrage du History Server..."
mr-jobhistory-daemon.sh start historyserver
sleep 5

# Attendre que les DataNodes se connectent
echo "Attente des DataNodes..."
sleep 20

# Vérifier l'état des DataNodes
echo "Etat des DataNodes:"
hdfs dfsadmin -report

# Créer les répertoires HDFS
echo "Création des répertoires HDFS..."
hdfs dfs -mkdir -p /user/root
hdfs dfs -mkdir -p /user/root/input
hdfs dfs -mkdir -p /user/root/output
hdfs dfs -chmod -R 755 /user/root

echo "Répertoires HDFS créés:"
hdfs dfs -ls /user/root

# Afficher l'état final du cluster
echo "Etat final du cluster:"
echo "Processus Java:"
jps
echo "Rapport HDFS:"
hdfs dfsadmin -report

echo "Cluster Hadoop initialisé avec succès"
echo "NameNode UI: http://localhost:9870"
echo "ResourceManager UI: http://localhost:8088"
EOF

# Rendre le script exécutable
chmod +x scripts/init-and-start-hadoop.sh

# Créer les répertoires de données
echo "Création des répertoires de données..."
mkdir -p hadoop-data/master
mkdir -p hadoop-data/slave1
mkdir -p hadoop-data/slave2
mkdir -p shared-data

# Démarrer les containers Docker
echo "Démarrage des containers Docker..."
docker-compose up -d

# Attendre le démarrage
echo "Attente du démarrage du cluster..."
sleep 30

# Vérifier l'état des containers
echo "Verification des containers:"
docker-compose ps

# Vérifier l'état du cluster Hadoop
echo "Verification du cluster Hadoop:"
docker exec hadoop-master jps
echo "---"
docker exec hadoop-master hdfs dfsadmin -report

# Tester la communication
echo "Test de communication:"
docker exec hadoop-master ping -c 2 hadoop-slave1
docker exec hadoop-master ping -c 2 hadoop-slave2

echo "Cluster Hadoop initialisé avec succès"
echo ""
echo "Commandes utiles:"
echo "  docker exec -it hadoop-master bash"
echo "  hdfs dfs -ls /user/root"
echo ""
echo "Interfaces Web:"
echo "  NameNode: http://localhost:9870"
echo "  ResourceManager: http://localhost:8088"