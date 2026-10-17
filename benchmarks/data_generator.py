"""Seeded deterministic data generator for guillotina_elasticsearch path benchmarks."""
import argparse, json, random, sys

def gen(n, seed=12345, max_depth=5, folders=20):
    random.seed(seed)
    for i in range(n):
        depth = random.randint(1, max_depth)
        parts = [f"d{random.randrange(folders)}" for _ in range(depth)]
        yield {"id": i, "path": "/" + "/".join(parts), "depth": depth}

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("n", type=int)
    p.add_argument("--seed", type=int, default=12345)
    p.add_argument("--out", default="-")
    args = p.parse_args()
    fh = sys.stdout if args.out == "-" else open(args.out, "w")
    for row in gen(args.n, seed=args.seed):
        fh.write(json.dumps(row) + "\n")
    if fh is not sys.stdout:
        fh.close()
