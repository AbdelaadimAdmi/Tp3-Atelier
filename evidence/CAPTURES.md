# Captures à insérer dans le rapport

Ouvrir le [rapport Word](../output/Rapport_Ateliers_Microservices.docx). Pour chaque figure, cliquer avec le bouton droit sur l’image de remplacement et choisir **Modifier l’image**, puis sélectionner le PNG correspondant. Dans Word, utiliser **Rogner > Ajuster** si nécessaire pour conserver toute la capture. Recadrer les zones inutiles dans l’Outil Capture d’écran avant l’insertion. Les commandes, le statut et les lignes de log doivent rester lisibles dans le PDF de deux pages.

Sous Windows, utiliser **Win + Maj + S**, sélectionner la zone utile et enregistrer la capture en PNG. Choisir une police de terminal assez grande pour que le texte reste lisible après insertion.

## Préparer les services

Ouvrir PowerShell à la racine du projet. Pour la pile complète, démarrer les conteneurs puis utiliser le port direct d’Order Service.

~~~powershell
docker compose up -d
docker compose ps
$base = 'http://127.0.0.1:8082'
~~~

En mode atelier 1 autonome, Order Service écoute sur 8081. Utiliser alors $base = 'http://127.0.0.1:8081'. Les commandes de démarrage de ce mode figurent dans le [README du projet](../README.md).

## Figure 1 Appel REST réussi

~~~powershell
$body = @{ customerEmail = 'client@example.com'; amount = 149.90 } | ConvertTo-Json -Compress
$r = Invoke-WebRequest "$base/api/orders" -Method Post -ContentType 'application/json' -Body $body
"POST $base/api/orders"
"HTTP $($r.StatusCode)"
$r.Content | ConvertFrom-Json | ConvertTo-Json
$id = ($r.Content | ConvertFrom-Json).orderId
~~~

Capturer la méthode POST, l’URL, la réponse **HTTP 201** et le JSON avec le champ orderId. Enregistrer sous **01-rest.png**.

## Figure 2 Logs de notification

Garder le même terminal pour réutiliser la valeur de $id. Si les lignes ne sont pas encore visibles, attendre quelques secondes puis relancer la commande.

~~~powershell
docker compose logs --no-color notification-service |
    Select-String -SimpleMatch $id |
    ForEach-Object { $_.Line }
~~~

Capturer les lignes **RabbitMQ reçu** et **Notification simulée** avec le même orderId que la figure 1. Enregistrer sous **02-notification.png**.

## Figure 3 File RabbitMQ et test d’erreur

1. Ouvrir [RabbitMQ Management](http://localhost:15672/#/queues) et se connecter avec atelier / atelier123. Dans **Queues and Streams**, repérer orders.notifications et orders.notifications.dlq. Relever le nombre de messages **Ready** dans la DLQ avant le test.
2. Envoyer une commande de montant 13 avec le consommateur en marche.

~~~powershell
$bad = @{ customerEmail = 'client@example.com'; amount = 13 } | ConvertTo-Json -Compress
$r = Invoke-WebRequest "$base/api/orders" -Method Post -ContentType 'application/json' -Body $bad
"HTTP $($r.StatusCode)"
$failedId = ($r.Content | ConvertFrom-Json).orderId
Start-Sleep -Seconds 2
docker compose logs --no-color notification-service |
    Select-String -SimpleMatch $failedId |
    Where-Object Line -like '*Échec RabbitMQ simulé*' |
    ForEach-Object { $_.Line }
~~~

3. Actualiser la page RabbitMQ. La DLQ doit avoir un message Ready de plus. Capturer les deux files et la colonne **Ready**. Si les trois lignes d’échec restent lisibles, placer la fenêtre PowerShell à côté de la page RabbitMQ et capturer les deux zones ensemble. Enregistrer sous **03-rabbitmq-dlq.png**. Le HTTP 201 correspond à la réception de la commande. L’échec survient ensuite dans le consommateur.

## Figure 4 Topic Kafka et offsets

Exécuter ces commandes après la réussite de la figure 1. Si le groupe est encore en cours d’affectation, attendre quelques secondes et relancer le bloc.

~~~powershell
& {
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic commandes-creees
    docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group atelier-notifications
} | Tee-Object -FilePath evidence/kafka-livrable.txt
~~~

Capturer le nom du topic commandes-creees, les trois partitions et les lignes du groupe pour ce topic, avec **CURRENT-OFFSET**, **LOG-END-OFFSET** et **LAG**. Si l’image est trop large, coller dans le rapport un extrait texte avec ces colonnes. Conserver **evidence/kafka-livrable.txt** comme sortie brute à remettre avec les sources et le rapport.
