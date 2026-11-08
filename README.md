# Atelier de microservices Spring Cloud

Les deux ateliers utilisent le même code. L’atelier 1 isole la création de commandes et les notifications. La suite ajoute le catalogue, la découverte des services, la configuration distante et la passerelle.

## Prérequis

- Docker Desktop démarré en mode conteneurs Linux et Docker Compose 2.24.4 ou supérieur. Prévoir environ 5 Go de mémoire pour Docker.
- Le build Docker inclut Maven 3.9 et Java 21. Pour compiler depuis un IDE ou un terminal, utiliser un JDK 21 et Maven 3.9.
- Python 3.10 ou supérieur est nécessaire uniquement pour les scripts de vérification.

Versions utilisées dans le projet :

| Technologie | Version |
| --- | --- |
| Spring Boot | 3.5.11 |
| Spring Cloud | 2025.0.1 |
| Kafka | 3.9.1 |
| RabbitMQ | 3 Management |

Le BOM Spring Cloud gère les versions des starters. Voir la [table de compatibilité Spring Cloud](https://spring.io/projects/spring-cloud/#overview).

## Démarrage

Exécuter les commandes suivantes depuis la racine du projet :

```powershell
docker compose up --build -d
docker compose ps
docker compose logs --tail 30 order-service notification-service
```

Compose démarre les serveurs de configuration et de découverte ainsi que les brokers. Les services dépendants démarrent après la disponibilité de leurs prérequis.

Attendre quelques secondes supplémentaires pour la propagation des inscriptions Eureka avant d’appeler la passerelle.

| Composant | Port hôte | Rôle ou point de contrôle |
| --- | --- | --- |
| API Gateway | 8080, ou 8180 sur ce poste | Routes `lb://PRODUCT-SERVICE` et `lb://ORDER-SERVICE` |
| Product Service | 8081 | Produit sur `/products/1` et configuration injectée sur `/products/config` |
| Order Service | 8082 | `POST /orders` et `POST /api/orders` |
| Notification Service | 8083 | Consommation RabbitMQ et Kafka. Santé sur `/actuator/health` |
| Eureka | 8761 | Tableau de bord et registre des services |
| Config Server | 8888 | Configuration native lue dans `config-repo` |
| RabbitMQ | 5672 et 15672 | AMQP et interface de gestion. Identifiants : `atelier` / `atelier123` |
| Kafka | 9092 | Bootstrap hôte : `localhost:9092`. Dans Compose : `kafka:29092` |

### Compilation et exécution hors Docker

`messaging-contracts` est une bibliothèque Java partagée. Elle contient le record d’événement et la topologie RabbitMQ. Chaque application est un module Maven et produit son propre JAR.

Pour compiler depuis la racine :

```powershell
mvn verify
```

Pour exécuter les applications localement, démarrer d’abord les brokers :

```powershell
docker compose up -d rabbitmq kafka
```

Exécuter ensuite chaque JAR dans un terminal séparé en remplaçant `MODULE` par le nom du module :

```powershell
java -jar MODULE/target/MODULE-1.0.0.jar
```

Respecter l’ordre de démarrage suivant :

1. Config Server et Eureka.
2. Product Service, Order Service et Notification Service.
3. API Gateway.

### Port de la passerelle

La passerelle écoute sur le port `8080` dans son conteneur.

Docker ayant refusé de publier le port hôte `8080` sur ce poste, le fichier local `.env` contient :

```dotenv
GATEWAY_PORT=8180
```

Sans ce réglage, le port hôte par défaut est `8080`.

### Atelier 1 aux ports d’origine

Arrêter d’abord la pile complète si elle est en cours d’exécution. Cette variante démarre uniquement les deux brokers et les deux services de messagerie.

```powershell
docker compose down

docker compose -f compose.yaml -f compose.workshop1.yaml up --build -d order-service notification-service

$basic = @{
    customerEmail = 'client@example.com'
    amount = 149.90
} | ConvertTo-Json

Invoke-WebRequest `
    -Uri 'http://localhost:8081/api/orders' `
    -Method Post `
    -ContentType 'application/json' `
    -Body $basic

docker compose logs --tail 30 notification-service
```

Pour arrêter cette variante :

```powershell
docker compose -f compose.yaml -f compose.workshop1.yaml down
```

Dans cette variante :

- Order Service utilise le port `8081` et le profil `standalone`.
- Notification Service utilise le port `8082`.

Dans la pile complète, la route `/api/orders` est accessible sur le port `8082`.

## Tests REST et configuration

Ces tests utilisent la pile complète.

### Vérifier la configuration et le catalogue

```powershell
Invoke-RestMethod -Uri 'http://localhost:8888/product-service/default'
Invoke-RestMethod -Uri 'http://localhost:8081/products/config'
Invoke-RestMethod -Uri 'http://localhost:8081/products/1'
```

### Tester la passerelle

Déterminer le port publié par Docker et construire une URL utilisant `localhost` :

```powershell
$gatewayBinding = docker compose port api-gateway 8080 | Select-Object -First 1

if (-not $gatewayBinding) {
    throw 'Aucun port publié pour api-gateway. Vérifiez que la pile est démarrée.'
}

$gatewayPort = ($gatewayBinding.Trim() -split ':')[-1]
$gateway = "http://localhost:$gatewayPort"

Invoke-RestMethod -Uri "$gateway/products/1"
```

### Créer une commande

```powershell
$order = @{
    productId = 1
    quantity = 2
    customerEmail = 'client@example.com'
} | ConvertTo-Json

# Appel direct à Order Service
Invoke-WebRequest `
    -Uri 'http://localhost:8082/orders' `
    -Method Post `
    -ContentType 'application/json' `
    -Body $order

# Appel via la passerelle
Invoke-WebRequest `
    -Uri "$gateway/orders" `
    -Method Post `
    -ContentType 'application/json' `
    -Body $order
```

Chaque appel POST crée une commande distincte.

Résultat attendu pour la commande catalogue :

- Statut HTTP : `201 Created`.
- État : `CREATED`.
- Produit : `Clavier`.
- Total : `499,80`.

Une quantité invalide renvoie HTTP `400`. Un produit inconnu renvoie HTTP `404`.

L’adresse e-mail est facultative sur `/orders` pour accepter l’exemple du sujet. Elle est obligatoire sur `/api/orders`.

### OpenFeign et WebClient

OpenFeign est actif par défaut.

Pour tester WebClient :

1. Dans `config-repo/order-service.yml`, remplacer la valeur `FEIGN` de la propriété `atelier.product.client` par `WEBCLIENT`.
2. Redémarrer Order Service :

   ```powershell
   docker compose restart order-service
   ```

3. Répéter les appels de création de commande.

Pour revenir à la configuration livrée, remettre `FEIGN` et redémarrer Order Service.

Feign décrit l’appel dans une interface annotée. WebClient construit la requête explicitement, avec `@LoadBalanced` et le nom logique `PRODUCT-SERVICE`. Il bloque à la frontière de l’API MVC avec une limite de 1,1 seconde.

Les deux variantes partagent le mapping des erreurs `404` et `503`, la politique de retry et le circuit breaker. Feign est conservé par défaut pour cet appel MVC unique.

### Simuler une panne de Product Service

```powershell
docker compose stop product-service

# Répéter le POST /orders via la passerelle au moins quatre fois.

docker compose logs --tail 40 order-service

docker compose start product-service

# Réessayer après la réinscription Eureka
# et l'expiration de la fenêtre ouverte de 10 secondes du circuit.
```

OpenFeign utilise :

- Un délai de connexion de `700 ms`.
- Un délai de lecture de `900 ms`.

Resilience4J autorise deux tentatives au total, séparées de `300 ms`, et compte un échec par opération ayant épuisé ses tentatives.

Le circuit breaker utilise les paramètres suivants :

| Paramètre | Valeur |
| --- | --- |
| Taille de la fenêtre | 5 opérations |
| Nombre minimum d’appels | 3 |
| Seuil d’échec | 50 % |
| Durée à l’état ouvert | 10 secondes |

Le fallback renvoie HTTP `503` avec un message explicite. Le `404` métier ne déclenche ni retry ni échec du circuit.

Pour tester le délai de lecture avec une connexion encore ouverte :

```powershell
docker compose pause product-service

# Appeler POST /orders pendant la pause.

docker compose unpause product-service
```

## RabbitMQ et Kafka

### RabbitMQ

Le producteur publie du JSON persistant dans la topologie suivante :

| Élément | Valeur |
| --- | --- |
| Exchange direct durable | `orders.exchange` |
| Clé de routage | `order.created` |
| File | `orders.notifications` |

Le HTTP `201` est renvoyé après la confirmation du broker. L’ACK du consommateur est envoyé après le traitement du message.

Pour vérifier la conservation des messages :

1. Arrêter Notification Service :

   ```powershell
   docker compose stop notification-service
   ```

2. Créer une commande.
3. Ouvrir l’[interface de gestion RabbitMQ](http://localhost:15672/#/queues).
4. Vérifier la colonne **Ready** de la file `orders.notifications`.
5. Redémarrer Notification Service et consulter ses logs :

   ```powershell
   docker compose start notification-service
   docker compose logs --tail 30 notification-service
   ```

### Échec de traitement et file de lettres mortes

Envoyer une commande catalogue de quantité `13` ou une commande simple de montant `13`.

Cela provoque trois tentatives RabbitMQ, puis un rejet sans remise en file. L’exchange `orders.dlx` achemine alors le message vers `orders.notifications.dlq`.

### Redélivrance

Pour tester la redélivrance :

1. Envoyer une commande catalogue de quantité `7` ou une commande simple de montant `7`.
2. Suivre les logs :

   ```powershell
   docker compose logs -f notification-service
   ```

3. Dès l’apparition du log `Traitement lent RabbitMQ`, exécuter dans un autre terminal :

   ```powershell
   docker compose kill notification-service
   docker compose start notification-service
   ```

Le consommateur signale ensuite `redelivered=true`.

Un fichier unique par `orderId` représente la notification et protège cette simulation d’un double effet après redémarrage.

### Kafka

Les routes publient les événements vers les topics suivants :

| Route | Topic |
| --- | --- |
| `/orders` | `commandes-creees` |
| `/api/orders` | `orders-events` |

Les événements utilisent `orderId` comme clé. Chaque topic possède trois partitions et un facteur de réplication de `1`.

Le groupe `atelier-notifications` journalise les événements sans simuler un deuxième envoi de notification.

En cas d’échec, le consommateur effectue deux nouvelles tentatives, puis publie l’événement vers le topic portant le suffixe `.DLT`. Le groupe confirme les offsets après un succès ou une récupération confirmée.

L’envoi Kafka est indépendant de la confirmation RabbitMQ. Une erreur Kafka est visible dans les logs.

Dans une version avec base de données, une outbox transactionnelle et un relais permettraient de reprendre les publications sans perdre la trace de la commande.

Pour inspecter les topics et le groupe de consommateurs :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic commandes-creees

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic orders-events

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group atelier-notifications
```

## Vérification et dépannage

### Scripts de vérification

Exécuter les scripts dans cet ordre, sur la pile complète et sans autres producteurs actifs :

```powershell
python scripts/verify.py
python scripts/kafka_groups.py
```

Le premier script interrompt temporairement Product Service et Notification Service pour tester la reprise.

Le second démarre des consommateurs de démonstration et republie un événement existant avec plusieurs clés de test.

Les sorties sont enregistrées dans `evidence/`.

### Problèmes courants

| Symptôme | Vérification |
| --- | --- |
| Port déjà utilisé | Exécuter `docker compose ps` et la commande PowerShell ci-dessous. Identifier le processus qui occupe le port avant de l’arrêter si nécessaire. |
| Gateway renvoie `503` au démarrage | Vérifier les inscriptions `PRODUCT-SERVICE` et `ORDER-SERVICE` dans Eureka, puis attendre une actualisation du registre. |
| Config Server inaccessible | Tester `/product-service/default` sur le port `8888`. Vérifier le montage de `config-repo` et les logs de Config Server. |
| RabbitMQ refuse la connexion | Vérifier la santé du broker et ses identifiants. Les files persistent dans le volume de l’atelier. |
| Aucun nouvel événement Kafka visible | Vérifier le topic, le bootstrap et le groupe. Un groupe existant reprend ses offsets enregistrés. Utiliser un groupe neuf avec `--from-beginning` pour relire le topic. |

Pour vérifier les ports sous Windows :

```powershell
Get-NetTCPConnection `
    -LocalPort 8080,8180,8081,8082,8083,8761,8888,5672,15672,9092 `
    -State Listen `
    -ErrorAction SilentlyContinue |
    Select-Object LocalAddress, LocalPort, OwningProcess
```

### Arrêt de la pile

```powershell
docker compose down
```

Cette commande arrête la pile et conserve les données de l’atelier.

Les brokers et les identifiants du projet sont réservés à la démonstration locale.
