#import functools
#tablica = list(map(int, input().split()))
#summa = functools.reduce(lambda x,y:x+y,tablica)
#print(functools.reduce(lambda x,y:x+y,tablica))
print('Wprowadz ostatnią liczbe ciągu:')
k = int(input())
def zadanie2(k):
    index = [1 for x in range(k+1)]
    for i in range(2,k+1):
        for j in range(i+1,k+1):
            if j%i == 0:
                index[j] = 0
    liczbyPierwsze = []
    for i in range(2,len(index)):
        if index[i] == 1:
            liczbyPierwsze.append(i)
            
    return liczbyPierwsze
        
print(zadanie2(k))     
        
                