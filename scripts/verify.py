"""Exercise the running Compose stack, including controlled service interruptions."""
import base64
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BROKER = "http://127.0.0.1:15672/api/queues/%2F/"
GROUP = "atelier-notifications"
BOOTSTRAP = "kafka:29092"
EVIDENCE = ROOT / "evidence"
AUTH = "Basic " + base64.b64encode(b"atelier:atelier123").decode()
QUEUE = "orders.notifications"
CATALOG = {"productId": 1, "quantity": 2, "customerEmail": "client@example.com"}
TRANSCRIPT = []


def note(message):
    print(message, flush=True)
    TRANSCRIPT.append(message)


def save(name, contents):
    EVIDENCE.mkdir(exist_ok=True)
    (EVIDENCE / name).write_text(contents.strip() + "\n", encoding="utf-8")


def compose(*args, input_text=None, timeout=55):
    command = ("docker", "compose", *args)
    return subprocess.run(command, cwd=ROOT, input=input_text, timeout=timeout,
                          text=True, encoding="utf-8", errors="replace", check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.strip()


def kafka(tool, *args, input_text=None):
    return compose("exec", "-T", "kafka", f"/opt/kafka/bin/kafka-{tool}.sh",
                   "--bootstrap-server", BOOTSTRAP, *args, input_text=input_text)


def http(url, data=None, auth=None):
    body = json.dumps(data).encode() if data is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    if auth:
        headers["Authorization"] = auth
    request = Request(url, data=body, headers=headers)
    try:
        result = urlopen(request, timeout=10)
    except HTTPError as error:
        result = error
    with result:
        return result.status, json.load(result)


def app(port, path, data=None):
    return http(f"http://127.0.0.1:{port}{path}", data)


def check(port, path, status, data=None):
    actual, payload = app(port, path, data)
    assert actual == status, (port, path, actual, payload)
    method = "GET" if data is None else "POST"
    note(f"{method} http://127.0.0.1:{port}{path}  ->  HTTP {actual}")
    if data is not None:
        note("  Requête " + json.dumps(data, ensure_ascii=False))
    note("  Réponse " + json.dumps(payload, ensure_ascii=False))
    return payload


def wait_for(label, predicate, timeout=55):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if result := predicate():
                return result
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            last = error
        time.sleep(0.75)
    raise AssertionError(f"{label} : délai dépassé ({last})")


def queue(name=QUEUE):
    status, data = http(BROKER + name, auth=AUTH)
    assert status == 200, data
    return data


def logs(service):
    return compose("logs", "--no-color", service)


def main():
    gateway_port = int(compose("port", "api-gateway", "8080").rsplit(":", 1)[-1])
    note("Vérification démarrée à " + datetime.now(timezone.utc).isoformat(timespec="seconds"))
    wait_for("Gateway et découverte", lambda: app(gateway_port, "/products/1")[0] == 200, 120)
    wait_for("Order Service", lambda: app(8082, "/actuator/health")[0] == 200, 120)
    wait_for("Notification Service", lambda: queue().get("consumers", 0) >= 1, 90)

    note("\n=== Discovery, Config Server et REST ===")
    eureka = Request("http://127.0.0.1:8761/eureka/apps", headers={"Accept": "application/json"})
    expected = {"PRODUCT-SERVICE", "ORDER-SERVICE", "API-GATEWAY"}

    def registered():
        with urlopen(eureka, timeout=10) as response:
            names = {item["name"] for item in json.load(response)["applications"].get("application", [])}
        return names if expected <= names else None

    names = wait_for("inscriptions Eureka", registered)
    note("Eureka : " + ", ".join(sorted(names)))
    config = check(8888, "/product-service/default", 200)
    welcome = next(source["source"]["message.bienvenue"] for source in config["propertySources"]
                   if "message.bienvenue" in source["source"])
    assert check(8081, "/products/config", 200)["bienvenue"] == welcome
    assert check(8081, "/products/1", 200) == check(gateway_port, "/products/1", 200)
    for port in (8082, gateway_port):
        order = check(port, "/orders", 201, CATALOG)
        assert order["status"] == "CREATED" and Decimal(str(order["total"])) == Decimal("499.80")
    basic = check(8082, "/api/orders", 201, {"customerEmail": "client@example.com", "amount": 149.90})
    check(8082, "/api/orders", 400, {"customerEmail": "incorrect", "amount": 0})
    check(gateway_port, "/orders", 400, {"productId": 1, "quantity": 0})
    check(gateway_port, "/orders", 404, {"productId": 999, "quantity": 1})
    wait_for("notification de base", lambda: f"Notification simulée orderId={basic['orderId']}"
             in logs("notification-service"))
    save("rest.txt", "\n".join(TRANSCRIPT))
    save("notification.txt", "\n".join(line for line in logs("notification-service").splitlines()
                                      if basic["orderId"] in line))

    note("\n=== RabbitMQ : arrêt, attente et reprise ===")
    wait_for("file vide", lambda: queue().get("messages", 0) == 0)
    compose("stop", "notification-service")
    try:
        queued = check(gateway_port, "/orders", 201, CATALOG)
        snapshot = wait_for("message en attente", lambda: q if
                            (q := queue()).get("messages_ready", 0) >= 1 else None)
        note(f"Notification arrêtée, file {QUEUE} : ready={snapshot['messages_ready']}")
    finally:
        compose("start", "notification-service")
    wait_for("consommation après reprise", lambda: f"Notification simulée orderId={queued['orderId']}"
             in logs("notification-service"))
    wait_for("ACK après reprise", lambda: queue().get("messages", 0) == 0)
    note("Après redémarrage : notification reçue, file vide après ACK.")

    before = queue(QUEUE + ".dlq").get("messages_ready", 0)
    failed = check(gateway_port, "/orders", 201, {**CATALOG, "quantity": 13})
    wait_for("mise en DLQ RabbitMQ", lambda: queue(QUEUE + ".dlq").get("messages_ready", 0) > before)
    failures = [line for line in logs("notification-service").splitlines()
                if "Échec RabbitMQ simulé" in line and failed["orderId"] in line]
    assert len(failures) == 3, failures
    note(f"DLQ RabbitMQ : {before} -> {queue(QUEUE + '.dlq')['messages_ready']}, "
         f"{len(failures)} tentatives.")
    for line in failures:
        note(line)

    slow = check(gateway_port, "/orders", 201, {**CATALOG, "quantity": 7})
    wait_for("traitement lent démarré",
             lambda: f"Traitement lent RabbitMQ orderId={slow['orderId']}" in logs("notification-service"),
             timeout=9)
    compose("kill", "notification-service")
    try:
        wait_for("message remis en file", lambda: queue().get("messages_ready", 0) >= 1)
    finally:
        compose("start", "notification-service")
    wait_for("redélivrance", lambda: f"orderId={slow['orderId']} redelivered=true" in logs("notification-service"))
    wait_for("notification après redélivrance", lambda: f"Notification simulée orderId={slow['orderId']}"
             in logs("notification-service"))
    note("Arrêt brutal pendant un traitement : redelivered=true, puis notification.")
    status, published = http("http://127.0.0.1:15672/api/exchanges/%2F/orders.exchange/publish",
                             {"properties": {"content_type": "application/json"}, "routing_key": "order.created",
                              "payload": json.dumps(basic), "payload_encoding": "string"}, auth=AUTH)
    assert status == 200 and published["routed"], published
    duplicate = f"Notification déjà traitée orderId={basic['orderId']}"
    wait_for("déduplication RabbitMQ", lambda: duplicate in logs("notification-service"))
    note("Republication du même orderId : notification déjà traitée, aucun second effet.")
    save("rabbitmq.txt", "\n".join(TRANSCRIPT[TRANSCRIPT.index("\n=== RabbitMQ : arrêt, attente et reprise ==="):]))

    note("\n=== Catalogue indisponible et rétablissement ===")
    previous_lines = len(logs("order-service").splitlines())
    # A paused process leaves its port open, so the test exercises the HTTP read timeout.
    compose("pause", "product-service")
    try:
        started = time.monotonic()
        response = check(gateway_port, "/orders", 503, CATALOG)
        assert "Commande non créée" in response.get("detail", ""), response
        elapsed = time.monotonic() - started
        assert 1.5 <= elapsed < 5, elapsed
        note(f"Produit bloqué : échec en {elapsed:.2f} s, avec deux appels tentés vers le catalogue.")
    finally:
        compose("unpause", "product-service")
    compose("stop", "product-service")
    try:
        for _ in range(3):
            check(gateway_port, "/orders", 503, CATALOG)
        assert any("CallNotPermittedException" in line
                   for line in logs("order-service").splitlines()[previous_lines:])
    finally:
        compose("start", "product-service")
    wait_for("circuit rétabli", lambda: app(gateway_port, "/orders", CATALOG)[0] == 201, 90)
    check(gateway_port, "/orders", 201, CATALOG)
    resilience = [line for line in logs("order-service").splitlines()[previous_lines:]
                  if re.search(r"Nouvelle tentative Product Service|Circuit productService|Fallback Product Service", line)]
    for transition in ("CLOSED to OPEN", "OPEN to HALF_OPEN", "HALF_OPEN to CLOSED"):
        assert any(f"State transition from {transition}" in line for line in resilience), transition
    for line in resilience:
        note(line)
    save("resilience.txt", "\n".join(TRANSCRIPT[TRANSCRIPT.index("\n=== Catalogue indisponible et rétablissement ==="):]))

    note("\n=== Kafka : partitions, commits et messages en erreur ===")
    for topic in ("commandes-creees", "orders-events"):
        description = kafka("topics", "--describe", "--topic", topic)
        assert "PartitionCount: 3" in description, description
        note(description)
    kafka_logs = wait_for("Kafka après DLT et reprise", lambda: kafka_summary(failed["orderId"], slow["orderId"]))
    for line in kafka_logs:
        note(line)
    offsets = kafka("consumer-groups", "--describe", "--group", GROUP)
    assert all(topic in offsets for topic in ("commandes-creees", "orders-events"))
    note(offsets)
    dlt_count = sum(int(line.rsplit(":", 1)[-1]) for line in
                    kafka("get-offsets", "--topic", "orders-events.DLT").splitlines()
                    if line.startswith("orders-events.DLT:"))
    dead_records = kafka("console-consumer", "--topic", "orders-events.DLT", "--from-beginning",
                         "--max-messages", str(dlt_count), "--timeout-ms", "7000",
                         "--property", "print.key=true", "--property", "print.partition=true",
                         "--property", "print.offset=true")
    assert failed["orderId"] in dead_records, dead_records
    note("Topic orders-events.DLT (commande du test) :")
    note(next(line for line in dead_records.splitlines() if failed["orderId"] in line))
    save("kafka.txt", "\n".join(TRANSCRIPT[TRANSCRIPT.index("\n=== Kafka : partitions, commits et messages en erreur ==="):]))

    note("\nTous les contrôles principaux ont réussi.")


def kafka_summary(failed, slow):
    lines = [line for line in logs("notification-service").splitlines()
             if ("Échec Kafka simulé" in line and failed in line)
             or ("Kafka reçu" in line and slow in line)]
    return lines if sum(failed in line for line in lines) == 3 and any(slow in line for line in lines) else None


if __name__ == "__main__":
    try:
        main()
    finally:
        save("verification.txt", "\n".join(TRANSCRIPT))







