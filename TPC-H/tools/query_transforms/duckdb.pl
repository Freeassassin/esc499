s/;\s*limit\s+(-?\d+);/($1 eq '-1') ? ';' : "\nlimit $1;"/ge;
s/;\s*where\s+rownum\s+<=\s*(-?\d+);/($1 eq '-1') ? ';' : "\nlimit $1;"/ge;
