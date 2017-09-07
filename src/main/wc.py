file_names = []
file_names.append("pg-being_ernest.txt")
file_names.append("pg-dorian_gray.txt")
file_names.append("pg-dracula.txt")
file_names.append("pg-emma.txt")
file_names.append("pg-frankenstein.txt")
file_names.append("pg-great_expectations.txt")
file_names.append("pg-grimm.txt")
file_names.append("pg-huckleberry_finn.txt")
file_names.append("pg-les_miserables.txt")
file_names.append("pg-metamorphosis.txt")
file_names.append("pg-moby_dick.txt")
file_names.append("pg-sherlock_holmes.txt")
file_names.append("pg-tale_of_two_cities.txt")
file_names.append("pg-tom_sawyer.txt")
file_names.append("pg-ulysses.txt")
file_names.append("pg-war_and_peace.txt")

s = 0

for file_name in file_names:
    now_s = 0
    with open(file_name) as f:
        for line in f.readlines():
            for word in line.split():
                # word = word.lower()
                if word == 'he':
                    s += 1
                    now_s += 1
    print file_name, now_s

print s
