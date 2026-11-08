# Preuves d’exécution

Voir le [guide de capture](CAPTURES.md) pour reproduire et insérer les quatre preuves du rapport.

Les captures et sorties proviennent de l’exécution locale avec la passerelle publiée sur le port 8180. Les fichiers texte gardent les identifiants de commande pour recouper les réponses et les logs.

| Fichier | Contrôle |
| --- | --- |
| [01-eureka.png](01-eureka.png) | Inscriptions de Gateway, Order et Product dans Eureka |
| [02-rest-notification.png](02-rest-notification.png) | Appels REST directs et via Gateway, réponse HTTP 201 et notification corrélée |
| [03-rabbitmq.png](03-rabbitmq.png) | File principale et file d’échec dans RabbitMQ Management |
| [rest.txt](rest.txt) et [notification.txt](notification.txt) | Réponses de configuration et REST, validation et notification |
| [rabbitmq.txt](rabbitmq.txt) | Accumulation, ACK, trois tentatives, DLQ, redélivrance et déduplication |
| [resilience.txt](resilience.txt) | Délai de lecture, HTTP 503, transitions du circuit et rétablissement |
| [kafka.txt](kafka.txt) | Description des topics, partitions, offsets de groupe et enregistrement en DLT |
| [kafka-groups.txt](kafka-groups.txt) | Clés identiques et différentes, répartition, second groupe et reprise |
| [webclient.txt](webclient.txt) et [standalone.txt](standalone.txt) | Variante WebClient et atelier 1 aux ports d’origine |
| [verification.txt](verification.txt) | Transcription des contrôles principaux |
