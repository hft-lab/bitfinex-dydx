with open('prices.txt', 'r') as file:
    data = file.read()
    data = data.split('\n')


max_diff = -100000
max_diff_rel = -100000
min_diff = 100000
min_diff_rel = 100000
diffs = []
diffs_relative = []

for record in data:
    try:
        diff = float(record.split(' ')[0]) - float(record.split(' ')[1])
    except:
        print(record)
        continue
    diff_rel = (diff / float(record.split(' ')[0])) * 100
    diffs.append(diff)
    diffs_relative.append(diff_rel)
    max_diff = diff if diff > max_diff else max_diff
    min_diff = diff if diff < min_diff else min_diff

    max_diff_rel = diff_rel if diff_rel > max_diff_rel else max_diff_rel
    min_diff_rel = diff_rel if diff_rel < min_diff_rel else min_diff_rel

print(f"Result for {len(diffs) / 360} hours")
print(f"Max price diff: {max_diff} USD")
print(f"Min price diff: {min_diff} USD")
print(f"Max price diff relative: {max_diff_rel} %")
print(f"Min price diff relative: {min_diff_rel} %")
print(f"Average diff: {sum(diffs) / len(diffs)}")
print(f"Average diff relative: {sum(diffs_relative) / len(diffs_relative)}")