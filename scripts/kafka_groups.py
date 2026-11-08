"""Show key affinity, partition assignment, independent groups and resumed offsets."""
from contextlib import ExitStack, contextmanager
import json
import re
import subprocess
from uuid import uuid4

from verify import BOOTSTRAP, ROOT, check, compose, kafka, save, wait_for

TOPIC = "commandes-creees"
FORMAT = ("--property", "print.partition=true", "--property", "print.offset=true",
          "--property", "print.key=true", "--property", "print.value=false")


@contextmanager
def member(group, client, reset):
    pid_file = f"/tmp/{group}-{client}.pid"
    shell = 'echo $$ > "$1"; shift; exec timeout 90 "$@"'
    command = ("docker", "compose", "exec", "-T", "kafka", "bash", "-c", shell,
               "member", pid_file, "/opt/kafka/bin/kafka-console-consumer.sh",
               "--bootstrap-server", BOOTSTRAP, "--topic", TOPIC, "--group", group,
               "--consumer-property", f"client.id={client}",
               "--consumer-property", f"auto.offset.reset={reset}",
               "--consumer-property", "auto.commit.interval.ms=500", *FORMAT)
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, encoding="utf-8", errors="replace")
    try:
        yield process
    finally:
        try:
            compose("exec", "-T", "kafka", "bash", "-c",
                    'if test -f "$1"; then kill -TERM "$(cat "$1")" 2>/dev/null || true; rm "$1"; fi',
                    "stop-member", pid_file)
            process.output, _ = process.communicate(timeout=12)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


def members(group, *clients):
    output = kafka("consumer-groups", "--describe", "--group", group, "--members", "--verbose")
    return output if all(client in output for client in clients) else None


def group_offsets(group):
    output = kafka("consumer-groups", "--describe", "--group", group)
    rows = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[:2] == [group, TOPIC]:
            partition, current, end, lag = fields[2:6]
            rows[int(partition)] = (current, end, lag)
    return output, rows


def fully_committed(group):
    output, rows = group_offsets(group)
    if len(rows) == 3 and all(end == "0" or current == end for current, end, _ in rows.values()):
        return output, rows
    return None


def publish(keys, event):
    value = json.dumps(event, separators=(",", ":"))
    kafka("console-producer", "--topic", TOPIC, "--property", "parse.key=true",
          input_text="".join(f"{key}\t{value}\n" for key in keys))


def record_count():
    offsets = kafka("get-offsets", "--topic", TOPIC)
    return sum(int(row.rsplit(":", 1)[-1]) for row in offsets.splitlines() if row.startswith(TOPIC + ":"))


def main():
    run = uuid4().hex[:8]
    shared = f"atelier-partage-{run}"
    independent = f"atelier-independant-{run}"
    event = check(8082, "/api/orders", 201, {"customerEmail": "client@example.com", "amount": 149.90})
    key = event["orderId"]
    varied = [f"{run}-cle-{n}" for n in range(8)]
    publish([key, key, *varied], event)

    sample = kafka("console-consumer", "--topic", TOPIC, "--from-beginning",
                   "--consumer-property", "enable.auto.commit=false", "--max-messages",
                   str(record_count()), "--timeout-ms", "12000", *FORMAT)
    parsed = [re.match(r"Partition:(\d+)\s+Offset:(\d+)\s+(\S+)", line) for line in sample.splitlines()]
    rows = [(int(match[1]), int(match[2]), match[3]) for match in parsed if match]
    same = [(part, offset) for part, offset, k in rows if k == key]
    different = [(part, offset, k) for part, offset, k in rows if k in varied]
    assert len(same) >= 2 and len({part for part, _ in same}) == 1, same
    assert len(different) == len(varied) and len({part for part, _, _ in different}) > 1, different
    lines = [f"Topic {TOPIC}", f"\nMême clé {key}", *[f"  Partition {p}, offset {o}" for p, o in same],
             "\nClés différentes", *[f"  Partition {p}, offset {o}, clé {k}" for p, o, k in different]]

    with ExitStack() as stack:
        stack.enter_context(member(shared, "member-a", "earliest"))
        stack.enter_context(member(shared, "member-b", "earliest"))
        assignment = wait_for("répartition entre deux consommateurs",
                              lambda: members(shared, "member-a", "member-b"))
        assigned = re.findall(r"\bmember-[ab]\s+[12]\s+\(([\d,]+)\)", assignment)
        partitions = [int(partition) for row in assigned for partition in row.split(",")]
        assert len(assigned) == 2 and sorted(partitions) == [0, 1, 2], assignment
        lines += ["\nDeux membres et leurs partitions", assignment]
        before, old_offsets = wait_for("offsets du groupe partagé", lambda: fully_committed(shared))
        lines += ["\nGroupe partagé avant arrêt", before]

    with member(independent, "independent", "latest") as independent_consumer:
        wait_for("groupe indépendant actif", lambda: members(independent, "independent"))
        # Confirm that the second group has an assignment before publishing a new record.
        time_marker = f"{run}-nouvel-offset"
        publish([time_marker], event)
        independent_output, _ = wait_for(
            "premier commit du groupe indépendant", lambda: fully_committed(independent))
    independent_records = [line for line in independent_consumer.output.splitlines()
                           if line.startswith("Partition:")]
    assert len(independent_records) == 1 and time_marker in independent_records[0], independent_records
    lines += ["\nGroupe indépendant démarré avec latest", independent_output, *independent_records]

    with member(shared, "member-restart", "earliest") as restarted_consumer:
        after, resumed = wait_for("reprise des offsets du groupe partagé", lambda: fully_committed(shared))
        assert sum(int(current) for current, _, _ in resumed.values() if current != "-") == (
            1 + sum(int(current) for current, _, _ in old_offsets.values() if current != "-")), resumed
    resumed_records = [line for line in restarted_consumer.output.splitlines() if line.startswith("Partition:")]
    assert len(resumed_records) == 1 and time_marker in resumed_records[0], resumed_records
    lines += ["\nGroupe partagé redémarré, un seul nouvel offset", after, *resumed_records]

    output = "\n".join(lines)
    print(output, flush=True)
    save("kafka-groups.txt", output)


if __name__ == "__main__":
    main()



