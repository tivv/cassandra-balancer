#!/usr/bin/env groovy
if (args.length != 1) {
    println("Usage: ${this.class.name} cassandra-host")
    System.exit(1);
}
def host = args[0];
boolean headerDone = false;

class Line {
    String address
    BigInteger token
    BigInteger range
    Line nextLine
    double koef
}
List<Line> lines = [];
BigInteger prevToken = new BigInteger(0)
["nodetool", "-h", host, "ring"].execute().in.eachLine {
    if (it.startsWith(" ")) {
        headerDone = true;
    } else if (headerDone) {
        def parsed = it.split("[ \t]+");
        if (parsed.length != 9) {
            println("Can't parse nodetool ring line: $it");
            System.exit(2);
        }
        if (parsed[3] != 'Up' || parsed[4] != 'Normal') {
            println("Ring is not in All Up/Normal state: ${parsed[0]} is in ${parsed[3]}/${parsed[4]}")
        }
        BigInteger token = new BigInteger(parsed[8])
        lines << new Line(address: parsed[0], token: token, range: token - prevToken)
        prevToken = token
    }
}
if (lines.size() < 2) {
    println("Ring is too small (${lines.size()})")
    System.exit(3)
}
BigInteger maxToken = BigInteger.TWO.pow(127)
lines[0].range += maxToken - lines.last().token
lines.eachWithIndex {line, i ->
    line.nextLine = lines[(i + 1) % lines.size()]
    line.koef = Math.abs(line.range.doubleValue() - line.nextLine.range.doubleValue())
    if (line.koef/line.range < 0.001) {
        //Already balanced
        line.koef = 0
    }
}
def maxLine = lines.max{it.koef}
if (maxLine.koef == 0) {
    println "Ring is already balanced."
    System.exit(0)
}
def moveTo = maxLine.token + (maxLine.nextLine.range - maxLine.range).shiftRight(1)
if (moveTo < 0) moveTo += maxToken
if (moveTo >= maxToken) moveTo -= maxToken
println("Balancing $maxLine.address with koef $maxLine.koef, moving from $maxLine.token to $moveTo")
Process process = ["nodetool", "-h", maxLine.address, "move", moveTo.toString()].execute()
process.consumeProcessOutput(System.out, System.err)
System.exit(process.waitFor())
