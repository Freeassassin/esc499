s/;\s*limit\s+(-?\d+);/($1 eq '-1') ? ';' : "\nlimit $1;"/ge;
s/;\s*where\s+rownum\s+<=\s*(-?\d+);/($1 eq '-1') ? ';' : "\nlimit $1;"/ge;
s/substring\(([^\)]+?) from ([0-9]+) for ([0-9]+)\)/substring($1, $2, $3)/gi;
s/extract\(year from ([^\)]+)\)/year($1)/gi;
