# %% [markdown]
# #### Partition the data by creating directory if not exist based on ptime attribute

# %%
import os, json, datetime

start_dt1 = datetime.datetime.strptime('2018-05-06 16:00:00', '%Y-%m-%d %H:%M:%S')
end_dt1 = datetime.datetime.strptime('2018-05-06 18:00:00', '%Y-%m-%d %H:%M:%S')
path = 'datalog.txt'

for file in os.listdir(path):
    # print(file)
    row_dict = []
    fp = os.path.join(path, file)
    f = open(fp, 'r')
    # print(fp)
    for line in f:
        # print(line)
        dto = None
        row = json.loads(line)
        dto = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
        # print(dto)
        part_ds = dto.strftime('%Y%m%d.txt')
        if not os.path.exists('partitioned_data'):
            os.makedirs('partitioned_data')
        part_fp = os.path.join('partitioned_data',part_ds)
        f2 = open(part_fp,'a')
        f2.write(line+"\n")
        f2.close()
        # break


# %% [markdown]
# #### Execute query 1: before partitioning
# #### 1. SELECT pitime,temp_5, temp_8 FROM print_reading WHERE pitime > 2018-05-06 16:00:00 AND pitime < 2018-05-06 18:00:00; 

# %%
start_dt1 = datetime.datetime.strptime('2018-05-06 16:00:00', '%Y-%m-%d %H:%M:%S')
end_dt1 = datetime.datetime.strptime('2018-05-06 18:00:00', '%Y-%m-%d %H:%M:%S')
path = 'datalog.txt'

final_data = []

start = datetime.datetime.now()

for file in os.listdir(path):
    # print(file)
    row_dict = []
    fp = os.path.join(path, file)
    f = open(fp, 'r')
    for line in f:
        # print(line)
        dto = None
        row = json.loads(line)
        dto = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
        # print(dto)
        if (dto >= start_dt1) and (dto <= end_dt1):
            row['temp_5'] = None if not 'temp_5' in row.keys() else row['temp_5']
            row['temp_8'] = None if not 'temp_8' in row.keys() else row['temp_8']
            final_data.append(({k : v for k, v in row.items() if k in ['temp_5', 'temp_8', 'pitime']}))
        # break

print(f'\nQuery 1: Time taken before partitioning: {datetime.datetime.now() - start}')

# %% [markdown]
# #### Execute query 1 after partitoning

# %%
path = 'partitioned_data'
final_data = []

start = datetime.datetime.now()

for file in os.listdir(path):
    fn = file[:8]
    try:
        dto_fn = datetime.datetime.strptime(fn, "%Y%m%d")
    except Exception as e:
        e
    
    if (dto_fn.date() >= start_dt1.date()) and (dto_fn.date() <= end_dt1.date()):
        fp = os.path.join(path, file)
        f = open(fp, 'r')
        for line in f:
            if not line.isspace():
                dto_row = None
                row = json.loads(line)
                dto_row = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
                if (dto >= start_dt1) and (dto <= end_dt1):
                    row['temp_5'] = None if not 'temp_5' in row.keys() else row['temp_5']
                    row['temp_8'] = None if not 'temp_8' in row.keys() else row['temp_8']
                    final_data.append(({k : v for k, v in row.items() if k in ['temp_5', 'temp_8', 'pitime']}))


print(f'Query 1: Time taken after partitioning: {datetime.datetime.now() - start}\n')

# %% [markdown]
# #### Execute query 2: before partitioning
# #### SELECT AVG(temp_8) FROM print_reading WHERE pitime > 2018-05-05 16:00:00 AND pitime < 2018-05-07 16:00:00;

# %%
start_dt2 = datetime.datetime.strptime('2018-05-05 16:00:00', '%Y-%m-%d %H:%M:%S')
end_dt2 = datetime.datetime.strptime('2018-05-07 16:00:00', '%Y-%m-%d %H:%M:%S')

path = 'datalog.txt'
final_data = []

start = datetime.datetime.now()

for file in os.listdir(path):
    # print(file)
    row_dict = []
    fp = os.path.join(path, file)
    f = open(fp, 'r')
    for line in f:
        # print(line)
        dto = None
        row = json.loads(line)
        dto = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
        # print(dto)
        if (dto >= start_dt2) and (dto <= end_dt2):
            row['temp_5'] = None if not 'temp_5' in row.keys() else row['temp_5']
            row['temp_8'] = None if not 'temp_8' in row.keys() else row['temp_8']
            final_data.append(({k : v for k, v in row.items() if k in ['temp_5', 'temp_8', 'pitime']}))
        # break
                
# print(final_data)

temp8_l = []
for row in final_data:
    dto = None
    dto = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
    if dto >= start_dt2 and dto <= end_dt2:
        if row['temp_8'] is not None:
            temp8_l.append(float(row['temp_8']))

temp8_avg = sum(temp8_l)/len(temp8_l)

print(f'Query 2: Time taken before partitioning: {datetime.datetime.now() - start}')

# %% [markdown]
# #### Execute query 2: after partitioning

# %%
path = 'partitioned_data'
final_data = []

start = datetime.datetime.now()
for file in os.listdir(path):
    fn = file[:8]
    try:
        dto_fn = datetime.datetime.strptime(fn, "%Y%m%d")
    except Exception as e:
        e
    
    if (dto_fn.date() >= start_dt2.date()) and (dto_fn.date() <= end_dt2.date()):
        fp = os.path.join(path, file)
        f = open(fp, 'r')
        for line in f:
            if not line.isspace():
                dto = None
                row = json.loads(line)
                dto = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
                if (dto >= start_dt2) and (dto <= end_dt2):
                    row['temp_5'] = None if not 'temp_5' in row.keys() else row['temp_5']
                    row['temp_8'] = None if not 'temp_8' in row.keys() else row['temp_8']
                    final_data.append(({k : v for k, v in row.items() if k in ['temp_5', 'temp_8', 'pitime']}))

temp8_l = []
for row in final_data:
    dto = None
    dto = datetime.datetime.strptime(row['pitime'], "%Y-%m-%dT%H:%M:%S.%f")
    if dto >= start_dt2 and dto <= end_dt2:
        if row['temp_8'] is not None:
            temp8_l.append(float(row['temp_8']))

temp8_avg = sum(temp8_l)/len(temp8_l)
print(f'Query 2: Time taken after partitioning: {datetime.datetime.now() - start}\n')


