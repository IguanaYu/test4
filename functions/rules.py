def rangeRule(data, check=True, **kwargs):
    rangeDict = kwargs.get('rangeDict')
    enumerationInterval = rangeDict.get('EI', [])
    rangeIntervals = rangeDict.get('RI', [])
    if len(enumerationInterval) + len(rangeIntervals) == 0:
        raise ValueError('未配置范围属性！')
    
    