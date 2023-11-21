import matplotlib.pyplot as plt
import pandas as pd
from functions.mysql import consanguinityAnalysis, conn

plt.rcParams['font.sans-serif'] = ['KaiTi','SimHei','FangSong']
plt.rcParams['axes.unicode_minus'] = False
plt.style.use('ggplot')

# 扩散系数
# 若扩散系数为0，则不扩散；若扩散系数大于0，则每级会扩散；若扩散系数小于0，则每级会收缩
spreadPara = 0
# 节点个数
nodeNum = 4
# 横坐标绝对值
absX = 2
# 节点间距
nodeSpace = 1
# 区间长度
intervalLen = nodeSpace * (nodeNum - 1) * (1 + absX * spreadPara)

node_id = 33
# 模拟数据来自mysql.py中获得的df1
# df2是对df1数据进行处理，获取各点横纵坐标
df1 = consanguinityAnalysis(node_id, 'uuu', conn)
df2 = df1[['node', 'level']].groupby('node').apply(lambda t: t[t.level.abs() == t.level.abs().max()]).drop_duplicates()
df2['rank'] = df2['level'].groupby(df2['level']).rank(method='first')
df2['count'] = df2['level'].groupby(df2['level']).rank(method='max')
df2['y'] = nodeSpace * (df2['count'] - 1) * (1 + abs(df2['level']) * spreadPara) * \
           (2 * df2['rank'] - df2['count'] - 1) / (2 * df2['count'] - 2)
df2['y'] = [0 if pd.isna(_) else _ for _ in df2['y']]

plt.figure()
plt.axis('off')
plt.title('节点%s的血缘关系图' % str(node_id))
for i in range(len(df2)):
    node = df2.iloc[i]
    name = node['node']
    x = node['level']
    y = node['y']
    if x == 0:
        c = 'r'
        s = 50
    else:
        c = 'b'
        s = 20
    plt.scatter(x, y, s=s, c=c)
    plt.annotate(name, (x, y), xytext=(5, 10), textcoords='offset points')
for i in range(len(df1)):
    row = df1.iloc[i]
    if row['level'] < 0:
        node1 = row['node']
        node2 = row['last_node']
    elif row['level'] > 0:
        node2 = row['node']
        node1 = row['last_node']
    else:
        continue
    node1_axis = list(df2[['level', 'y']][df2['node'] == node1].iloc[0])
    node2_axis = list(df2[['level', 'y']][df2['node'] == node2].iloc[0])
    plt.quiver(node1_axis[0] + (node2_axis[0] - node1_axis[0]) * 0.1,
               node1_axis[1] + (node2_axis[1] - node1_axis[1]) * 0.1,
               (node2_axis[0] - node1_axis[0]) * 0.8,
               (node2_axis[1] - node1_axis[1]) * 0.8,
               angles='xy', scale_units='xy', scale=1, color='y')
plt.show()
