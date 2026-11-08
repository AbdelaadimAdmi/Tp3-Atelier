# Atelier de microservices Spring Cloud

Les deux ateliers utilisent le même code. L’atelier 1 isole la création de commande et la notification. La suite ajoute le catalogue, la découverte, la configuration distante et la passerelle.

## Prérequis

- Docker Desktop démarré en mode conteneurs Linux et Docker Compose 2.24.4 ou supérieur. Prévoir environ 5 Go de mémoire pour Docker.
- Le build Docker inclut Maven 3.9 et Java 21. Pour compiler depuis un IDE ou le terminal, utiliser un JDK 21 et Maven 3.9.
- Python 3.10 ou supérieur est nécessaire uniquement pour les scripts de vérification.

Versions utilisées dans le projet. Spring Boot 3.5.11, Spring Cloud 2025.0.1, Kafka 3.9.1 et RabbitMQ 3 Management. Le BOM Spring Cloud gère les versions des starters. [Table de compatibilité Spring Cloud](https://spring.io/projects/spring-cloud/#overview)

## Démarrage

Depuis la racine du projet.

```powershell
docker compose up --build -d
docker compose ps
docker compose logs --tail 30 order-service notification-service
```

Compose démarre les serveurs de configuration et de découverte ainsi que les brokers. Les services dépendants démarrent après la disponibilité de leurs prérequis. Attendre quelques secondes supplémentaires pour la propagation des inscriptions Eureka avant d’appeler la passerelle.

| Composant | Port | Rôle ou point de contrôle |
| --- | ---: | --- |
| API Gateway | 8080 (8180 sur ce poste) | Routes `lb://PRODUCT-SERVICE` et `lb://ORDER-SERVICE` |
| Product Service | 8081 | Produit `/products/1` et configuration injectée `/products/config` |
| Order Service | 8082 | `POST /orders` et `POST /api/orders` |
| Notification Service | 8083 | Consommation RabbitMQ et Kafka. Santé sur `/actuator/health` |
| Eureka | 8761 | Tableau de bord et registre des services |
| Config Server | 8888 | Configuration native lue dans `config-repo` |
| RabbitMQ | 5672, 15672 | AMQP et interface de gestion. Identifiants `atelier` / `atelier123` |
| Kafka | 9092 | Bootstrap hôte `localhost:9092`. Dans Compose, `kafka:29092` |

`messaging-contracts` est une bibliothèque Java partagée. Elle contient le record d’événement et la topologie RabbitMQ. Chaque application est un module Maven et produit son propre JAR. La compilation hors Docker se lance à la racine avec `mvn verify`. Pour lancer un JAR depuis la racine, démarrer les brokers avec `docker compose up -d rabbitmq kafka`, puis exécuter chaque JAR dans un terminal séparé avec `java -jar MODULE/target/MODULE-1.0.0.jar`. Démarrer Config et Eureka, puis Product, Order et Notification, puis Gateway.

La passerelle écoute dans son conteneur sur 8080. Docker a refusé de publier le port hôte 8080 sur ce poste. Le fichier local `.env` fixe donc `GATEWAY_PORT=8180`. Sans ce réglage, le port hôte par défaut est 8080.

### Atelier 1 aux ports d’origine

Arrêter d’abord la pile complète si elle tourne. Cette variante démarre seulement les deux brokers et les deux services de messagerie.

```powershell
docker compose down
docker compose -f compose.yaml -f compose.workshop1.yaml up --build -d order-service notification-service
$basic = @{ customerEmail = 'client@example.com'; amount = 149.90 } | ConvertTo-Json
Invoke-WebRequest http://localhost:8081/api/orders -Method Post -ContentType 'application/json' -Body $basic
docker compose logs --tail 30 notification-service
docker compose -f compose.yaml -f compose.workshop1.yaml down
```

Order utilise le port 8081 et le profil `standalone`, Notification le port 8082. `/api/orders` reste accessible sur le port 8082 dans la pile complète.

## Tests REST et configuration

```powershell
Invoke-RestMethod http://localhost:8888/product-service/default
Invoke-RestMethod http://localhost:8081/products/config
Invoke-RestMethod http://localhost:8081/products/1
$gateway = "http://$(docker compose port api-gateway 8080)"
Invoke-RestMethod "$gateway/products/1"
$order = @{ productId = 1; quantity = 2; customerEmail = 'client@example.com' } | ConvertTo-Json
Invoke-WebRequest http://localhost:8082/orders -Method Post -ContentType 'application/json' -Body $order
Invoke-WebRequest "$gateway/orders" -Method Post -ContentType 'application/json' -Body $order
```

Attendu pour la commande catalogue. HTTP 201, état `CREATED`, Clavier, total 499,80. Une quantité invalide donne 400, un produit inconnu donne 404. L’adresse e-mail est facultative sur `/orders` pour accepter l’exemple du sujet. Elle est obligatoire sur `/api/orders`.

### OpenFeign et WebClient

OpenFeign est actif par défaut. Pour exécuter la variante, passer `atelier.product.client` de `FEIGN` à `WEBCLIENT` dans `config-repo/order-service.yml`, puis exécuter `docker compose restart order-service` et répéter les appels. Remettre `FEIGN` et redémarrer pour revenir à la configuration livrée.

Feign décrit l’appel dans une interface annotée. WebClient construit la requête explicitement, avec `@LoadBalanced` et le nom logique `PRODUCT-SERVICE`. Il bloque à la frontière de l’API MVC avec une limite de 1,1 s. Les deux variantes partagent le mapping 404/503, la politique de retry et le circuit. Feign est conservé comme valeur par défaut pour cet appel MVC unique.

### Panne produit

```powershell
docker compose stop product-service
# Répéter le POST /orders via la passerelle au moins quatre fois.
docker compose logs --tail 40 order-service
docker compose start product-service
# Réessayer POST /orders après réinscription Eureka et la fenêtre de 10 s du circuit.
```

OpenFeign utilise un délai de connexion de 700 ms et un délai de lecture de 900 ms. Resilience4J autorise deux tentatives séparées de 300 ms et compte un échec par opération épuisée. Le circuit utilise une fenêtre de cinq opérations, un minimum de trois appels, un seuil d’échec de 50 % et une fenêtre ouverte de 10 s. Le fallback renvoie HTTP 503 avec un message explicite. Le 404 métier ne déclenche ni retry ni échec du circuit. Pour exercer le délai de lecture avec une connexion encore ouverte, utiliser `docker compose pause product-service`, appeler la route, puis `docker compose unpause product-service`.

## RabbitMQ et Kafka

- RabbitMQ. Le producteur publie du JSON persistant dans l’exchange direct durable `orders.exchange`, clé `order.created`, file `orders.notifications`. Le HTTP 201 suit la confirmation du broker. L’ACK du consommateur suit son traitement. Arrêter Notification, créer une commande et consulter la colonne Ready à l’adresse `http://localhost:15672/#/queues`, puis redémarrer Notification et vérifier le log.
- Envoyer une commande de quantité 13 ou une commande simple de montant 13 pour provoquer trois tentatives RabbitMQ, puis un rejet sans remise en file. L’exchange `orders.dlx` achemine le message vers `orders.notifications.dlq`.
- Pour la redélivrance, choisir la quantité 7 ou le montant 7. Au log `Traitement lent RabbitMQ`, exécuter `docker compose kill notification-service` puis `docker compose start notification-service`. Le consommateur signale `redelivered=true`. Un fichier unique par `orderId` représente la notification et protège cette simulation d’un double effet après redémarrage.
- Kafka. Les deux routes publient respectivement vers `commandes-creees` et `orders-events`, avec `orderId` comme clé, trois partitions et réplication 1. Le groupe `atelier-notifications` journalise les événements, sans simuler un deuxième envoi. En cas d’échec, il effectue deux nouvelles tentatives puis publie vers le topic `.DLT`. Le groupe confirme les offsets après succès ou récupération confirmée.
- L’envoi Kafka est indépendant de la confirmation RabbitMQ. Une erreur Kafka est visible dans les logs. Dans une version avec base de données, une outbox transactionnelle et un relay permettraient de reprendre les publications sans perdre la trace de la commande.

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic commandes-creees
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic orders-events
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group atelier-notifications
```

## Vérification et dépannage

```powershell
python scripts/verify.py
python scripts/kafka_groups.py
```

Lancer les scripts dans cet ordre sur la pile complète et sans autres producteurs actifs. Le premier interrompt temporairement Product et Notification pour les tests de reprise. Le second démarre des consommateurs de démonstration et republie un événement existant avec plusieurs clés de test. Les sorties sont écrites dans `evidence/`.

| Symptôme | Vérification |
| --- | --- |
| Port déjà utilisé | Exécuter `docker compose ps` et `Get-NetTCPConnection -LocalPort 8080,8081,8082,8083,8761,8888,5672,15672,9092`. Arrêter le processus local qui occupe le port si nécessaire. |
| Gateway 503 au démarrage | Vérifier les inscriptions `PRODUCT-SERVICE` et `ORDER-SERVICE` sur Eureka, puis attendre une actualisation du registre. |
| Config inaccessible | Tester `/product-service/default` sur 8888. Vérifier le montage `config-repo` et les logs Config Server. |
| RabbitMQ refuse la connexion | Vérifier la santé du broker et ses identifiants. Les files persistent dans le volume de l’atelier. |
| Kafka sans nouveaux événements visibles | Vérifier le topic, le bootstrap et le groupe. Un groupe existant reprend ses offsets enregistrés. Utiliser un groupe neuf avec `--from-beginning` pour relire le topic. |

`docker compose down` arrête la pile et conserve les données de l’atelier. Les brokers et les identifiants du projet sont réservés à la démonstration locale.


## Livrables

Le [rapport de deux pages](output/pdf/Rapport_Ateliers_Microservices.pdf) répond aux questions de l’atelier 1. Utiliser le [rapport Word modifiable](output/Rapport_Ateliers_Microservices.docx) et le [guide de capture](evidence/CAPTURES.md) pour insérer les preuves avant l’export. L’[index des preuves](evidence/README.md) contient les captures et les sorties déjà enregistrées.

#   T p 3 - A t e l i e r  
 