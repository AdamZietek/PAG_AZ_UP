print('Wprowadz sumę:')
summa = int(input())
print('Wprowadz ciąg liczb do sprawdzenia:')
tablica = list(map(int, input().split())) 
def zadanie1(summa,tablica):
    for i in range(len(tablica)):
        for j in range(i+1,len(tablica)):
            if tablica[i]+tablica[j] == summa:
                return True
    for i in range(len(tablica)):
        if tablica[i] == summa:
            return True
    return False
print(zadanie1(summa,tablica))

        
        
   


