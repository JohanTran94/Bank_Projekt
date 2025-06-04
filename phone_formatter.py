import csv
import re


def format_phone_number(phone):
    """Ser till att telefonnumret börjar med +46 och tar bort otillåtna specialtecken."""
    phone = re.sub(r"[^\d+]", "", phone)  # Tar bort allt utom siffror och "+"

    if not phone.startswith("+46"):
        if phone.startswith("0"):
            phone = "+46" + phone[1:]  # Byt ut inhemskt format till +46
        else:
            phone = "+46" + phone  # Lägg till landskod

    return phone


# Läs och justera CSV-filen
input_file = "bank_data.csv"  # Din CSV-fil
output_file = "formatted_bank_data.csv"

with open(input_file, mode="r", encoding="utf-8") as infile, open(output_file, mode="w", encoding="utf-8",
                                                                  newline='') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    header = next(reader)  # Läs första raden (rubriker)
    writer.writerow(header)

    for row in reader:
        if row[4].startswith("SE8902"):  # Kontrollera om kontonumret börjar med SE8902
            row[2] = format_phone_number(row[2])  # Använd regex för att rensa och formatera telefonnumret

        writer.writerow(row)

print("Filformatering klar! Data sparad i", output_file)
