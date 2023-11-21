import base64
import binascii
import string
import json
import time
from hashlib import md5

import secrets
from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
from Crypto.Cipher import PKCS1_v1_5 as pkcs1Cipher
from Crypto.Signature import PKCS1_v1_5 as pkcs1Signature


# import configparser as cp

# iniConfig = cp.ConfigParser()
# iniPath = 'functions/ini/Key.ini'
# iniConfig.read(iniPath)
# iniKey = dict(iniConfig.items('SQL_ADMIN'))['key']
def MD5Salt(stringToBeEncode, salt1=None, salt2=None, encoding='utf-8'):
    if salt1 is None:
        salt1 = 'qzkj'
    if salt2 is None:
        salt2 = 'sjzl'
    if stringToBeEncode is None:
        stringToBeEncode = ''
    return md5((salt1 + stringToBeEncode + salt2).encode(encoding)).hexdigest()


pad = {'blank': lambda x, y, z: (x + (y - len(x) % y) % y * ' ').encode(z),
       'PKCS7': lambda x, y, z: (x + (y - len(x) % y) * chr(y - len(x) %
                                                            y)).encode(z)}
unpad = {'blank': lambda x: x.rstrip(),
         'PKCS7': lambda x: x[:-x[-1]]}
encode = {'base64': base64.b64encode,
          'hex': binascii.b2a_hex}
decode = {'base64': base64.b64decode,
          'hex': binascii.a2b_hex}
methodDict = {'ECB': AES.MODE_ECB, 'CBC': AES.MODE_CBC, 'CFB': AES.MODE_CFB,
              'CTR': AES.MODE_CTR, 'OFB': AES.MODE_OFB,
              'MD5': lambda x: md5(x.encode('utf-8')).hexdigest(),
              'MD5SALT': MD5Salt}
blockSize = AES.block_size


def randSeqStr(item, n=1):
    """
    生成随机字符串
    :param item: 字符串的元素空间
    :param n: 字符串长度
    :return: 随机字符串，字符串中的每个元素均来自元素空间
    """
    while True:
        if n == 1:
            yield secrets.choice(item)
        else:
            yield ''.join([secrets.choice(item) for _ in range(n)])


def stringPadding(toLength, strGiven='', padStr=None, mode='hex'):
    """
    若修饰后长度不小于待修饰字符串长度，则取待修饰字符串前修饰后长度位；
    否则，用修饰字符在带修饰字符串后进行填充，直至字符串长度等于修饰后长度
    :param toLength: 字符串修饰后长度
    :param strGiven: 待修饰的字符串
    :param padStr: 用于修饰字符串的单字符，默认为空格
    :param mode: 随机生成
    :return: 修饰后的字符串
    """
    strGiven = str(strGiven)
    modeDict = {'hex': '0123456789abcdef',
                'base64': string.ascii_letters + string.digits + '========'}
    try:
        stringSource = modeDict[mode]
    except Exception:
        stringSource = string.ascii_letters + string.digits \
                       + string.punctuation
    if not isinstance(toLength, int):
        raise Exception('扩充后字符长度应为整数！')
    if len(strGiven) >= toLength:
        strRes = strGiven[:toLength]
    else:
        if padStr is None:
            randLetters = next(randSeqStr(stringSource,
                                          toLength - len(strGiven)))
            strRes = strGiven + randLetters
        else:
            strRes = strGiven + padStr * \
                     ((toLength - len(strGiven) + 1) // len(padStr))
            strRes = strRes[:toLength]
    return strRes


def keyGenerate(key, year, month, day, method='MD5'):
    """
    用于将任意长度密钥转换为标准32位字符串密钥，用于AES对称加密
    :param key: 原始字符串密钥
    :param year: 日期年
    :param month: 日期月
    :param day: 日期日
    :param method: 加密算法
    :return: 加密后标准32位字符串密钥
    """
    key = str(key)
    year = str(year)
    month = str(month) if len(str(month)) == 2 else '0' + str(month)
    day = str(day) if len(str(day)) == 2 else '0' + str(day)
    fullKey = str(year) + key[:int(len(key))] + \
              str(month) + key[int(len(key)):] + str(day)
    if method not in methodDict.keys():
        raise Exception('加密算法不在可选范围内！')
    return methodDict[method](fullKey)


def base64urlDecode(base64String):
    size = len(base64String) % 4
    if size == 2:
        base64String += '=='
    elif size == 3:
        base64String += '='
    elif size != 0:
        raise ValueError('非法base64字符串！')
    return base64.urlsafe_b64decode(base64String.encode('utf-8'))


class CipherAES:
    
    def __init__(self, counter=None, cipherMethod='CBC',
                 padMethod='PKCS7', codeMethod='base64', encoding='utf-8'):
        """
        创建cipherAES类，设定初始参数
        :param counter: 用于CTR模式的计数器，可通过外部提供，也可使用内置的计数器
        :param cipherMethod: AES加/解密算法
        :param padMethod: 待加密文本填充算法
        :param codeMethod: 密文编码算法
        :param encoding: 待加密文本编码
        """
        self.encoding = encoding
        self.method = cipherMethod.upper() \
            if cipherMethod and isinstance(cipherMethod, str) else 'ECB'
        self.padMethod = padMethod
        self.codeMethod = codeMethod
        self.ctr = counter
        self.iv = ''
    
    def keyGen(self, key=None):
        if isinstance(key, bytes):
            return key
        elif isinstance(key, str) or (not key):
            return key.encode(self.encoding) if key \
                else 'QZDataManagement'.encode(self.encoding)
    
    def ivGen(self, iv=None):
        if isinstance(iv, str) or (not iv):
            return stringPadding(blockSize, iv, mode=self.codeMethod).encode(
                self.encoding) if iv else stringPadding(
                blockSize, mode=self.codeMethod).encode(self.encoding)
        elif isinstance(iv, bytes):
            return iv
    
    def counter(self, iv, posStart=1, posMove=0, addNum=1):
        """
        生成用于CTR模式使用的内置计数器，可通过自行指定初始字符串、类的初始偏移位、偏移位增量、
        偏移位值增量参数实现自定义计数器
        :param iv: 为计数器提供的初始字符串
        :param posStart: 字符串的初始偏移位
        :param posMove: 每次生成字符串后的偏移位增量
        :param addNum: 每次生成字符串时偏移位值增量
        :return: 计数器，根据提供的初始字符串、初始偏移位、偏移位增量、偏移位值增量，
                每次运行时生成一串用于CTR模式与字段块混合加/解密的字节组
        【吐槽】为啥要采用这种麻烦的形式，因为CTR模式的cipher要求的counter参数必须是一个可
        被调用的对象，即一个函数，不带括号的那种，例如b'aes'.upper；但内部的黑盒子又要求这个
        对象每次被调用后返回一串字节型字符串，即这个函数带括号后运行后能返回要求的输出，例如
        b'aes'.upper()。可以看出来这中间是没法传参的。但计数器有要求产生一系列的符合某种条件
        变动的字节型字符串，所以只能采用这种麻烦的形式。
        """
        
        def counterYield():
            textBytes = iv
            position = posStart % blockSize
            stepValue = posMove
            addValue = addNum
            while True:
                element = (textBytes[position - 1] + addValue) % 128
                textBytes = textBytes[:position - 1] + chr(element) \
                    .encode(self.encoding) + textBytes[position:]
                position = (position + stepValue) % blockSize
                position = blockSize if position == 0 else position
                yield textBytes
        
        ctr = counterYield()
        
        def counterRun():
            return next(ctr)
        
        return counterRun
    
    def sliceToBeEncoded(self, text, key, iv, posStart, posMove, addNum):
        if self.method in ['CFB', 'CBC', 'OFB']:
            cipher = AES.new(key, methodDict[self.method], iv)
        elif self.method == 'CTR' and self.ctr:
            cipher = AES.new(key, methodDict[self.method],
                             counter=self.ctr)
        elif self.method == 'CTR':
            ctr = self.counter(iv, posStart, posMove, addNum)
            cipher = AES.new(key, methodDict[self.method], counter=ctr)
        else:
            cipher = AES.new(key, methodDict[self.method])
        keyLen = len(key)
        while len(text) > keyLen:
            textSlice = text[:keyLen]
            text = text[keyLen:]
            encodedTextSlice = cipher.encrypt(textSlice.encode(self.encoding))
            yield encodedTextSlice
        lastTextSlice = pad[self.padMethod](text, keyLen, self.encoding)
        encryptedSlice = cipher.encrypt(lastTextSlice)
        yield encryptedSlice
    
    def sliceToBeDecoded(self, textEncoded, key, iv,
                         posStart, posMove, addNum):
        if self.method in ['CFB', 'CBC', 'OFB']:
            cipher = AES.new(key, methodDict[self.method], iv)
        elif self.method == 'CTR' and self.ctr:
            cipher = AES.new(key, methodDict[self.method], counter=self.ctr)
        elif self.method == 'CTR':
            ctr = self.counter(iv, posStart, posMove, addNum)
            cipher = AES.new(key, methodDict[self.method], counter=ctr)
        else:
            cipher = AES.new(key, methodDict[self.method])
        textBytes = decode[self.codeMethod](textEncoded.encode(self.encoding))
        keyLen = len(key)
        while len(textBytes) > keyLen:
            textBytesSlice = textBytes[:keyLen]
            textBytes = textBytes[keyLen:]
            decodedTextSlice = cipher.decrypt(textBytesSlice)
            yield decodedTextSlice
        lastTextBytesSlice = cipher.decrypt(textBytes)
        yield unpad[self.padMethod](lastTextBytesSlice)
    
    def encrypt(self, text, key=None, iv=None,
                posStart=1, posMove=0, addNum=1):
        key = self.keyGen(key)
        iv = self.ivGen(iv)
        try:
            textSlices = self.sliceToBeEncoded(text, key, iv,
                                               posStart, posMove, addNum)
            textEncoded = b''.join([_ for _ in textSlices])
            ivSlice = iv.decode(self.encoding)
            self.iv = ivSlice
            return ivSlice[:8] + encode[self.codeMethod](textEncoded).decode(
                self.encoding) + ivSlice[8:]
        except Exception:
            return 'error'
    
    def decrypt(self, textEncoded, key=None, iv=None,
                posStart=1, posMove=0, addNum=1):
        iv = self.ivGen(iv)
        try:
            textSlices = self.sliceToBeDecoded(textEncoded, key, iv,
                                               posStart, posMove, addNum)
            textDecoded = b''.join([_ for _ in textSlices])
            return textDecoded.decode(self.encoding)
        except Exception as e:
            return str(e) + '\n' + e.__doc__


def rsaKeyGen(path="functions/ini/"):
    rsaKey = RSA.generate(2048)
    with open(path + 'publicKey.txt', 'w') as f1:
        f1.write(rsaKey.public_key().export_key().decode())
    with open(path + 'privateKey.txt', 'w') as f2:
        f2.write(rsaKey.export_key().decode())
    

# 43200是半天，604800是1周
def makeJwtTokenRS256(info, timeRange=604800, keyFile=None):
    if not keyFile:
        keyFile = 'functions/ini/privateKey.txt'
    privateKey = RSA.importKey(open(keyFile).read())
    header = {
        'typ': 'JWT',
        'alg': 'RS256'
        }
    currentTime = int(time.time())
    payload = {
        'iss': '清众科技',
        'sub': 'testUser',
        'iat': currentTime,
        'exp': currentTime + timeRange,
        'nbf': currentTime,
        'aud': '数据治理',
        'jti': MD5Salt(str(info))
        }
    if not isinstance(info, dict):
        raise TypeError
    payload.update(info)
    header64 = json.dumps(header).encode('utf-8')
    payload64 = json.dumps(payload).encode('utf-8')
    headerEncoded = base64.urlsafe_b64encode(header64).decode('utf-8')
    payloadEncoded = base64.urlsafe_b64encode(payload64).decode('utf-8')
    signature = headerEncoded + '.' + payloadEncoded
    sigClass = pkcs1Signature.new(privateKey)
    msg = SHA256.new(signature.encode())
    signatureEncoded = base64.urlsafe_b64encode(sigClass.sign(msg)).decode()
    jwtToken = signature + '.' + signatureEncoded
    return jwtToken
    

class JwtTokenRS256:
    
    def __init__(self, jwtToken, publicKeyFile):
        if not isinstance(jwtToken, str):
            raise ValueError('应提供字符串格式的token！')
        try:
            self.jwtTokenList = jwtToken.split('.')
            if len(self.jwtTokenList) != 3:
                raise ValueError('Token格式不是JWT格式！')
        except Exception:
            raise ValueError('Token格式不是JWT格式！')
        try:
            self.publicKey = RSA.importKey(open(publicKeyFile).read())
        except Exception:
            raise ValueError('请提供正确的公钥文本文件！')
    
    def parseJWT(self):
        try:
            header = base64urlDecode(self.jwtTokenList[0]).decode()
            payload = base64urlDecode(self.jwtTokenList[1]).decode()
            res = {
                'header': json.loads(header),
                'payload': json.loads(payload),
                'signature': self.jwtTokenList[-1]
                }
            return res
        except Exception:
            raise ValueError('Token格式不是JWT格式！')
    
    def publicCheck(self):
        message = self.jwtTokenList[0] + '.' + self.jwtTokenList[1]
        sign = self.jwtTokenList[-1]
        signBytes = base64.urlsafe_b64decode \
            ((sign + '=' * (4 - len(sign) % 4)).encode())
        signature = pkcs1Signature.new(self.publicKey)
        shaHash = SHA256.new(message.encode())
        return signature.verify(shaHash, signBytes)
    
    def jwtCheck(self):
        if not self.publicCheck():
            msg = 'jwt验签未通过！'
            state = False
            return state, msg
        tokenInfo = self.parseJWT()['payload']
        currentTime = time.time()
        try:
            tokenEffectedTime = tokenInfo['nbf']
            tokenAbortedTime = tokenInfo['exp']
            if tokenAbortedTime < tokenEffectedTime \
                    or currentTime > tokenAbortedTime \
                    or currentTime < tokenEffectedTime:
                msg = 'JWT已失效！'
                state = False
            else:
                msg = tokenInfo
                state = True
        except Exception:
            msg = '非法JWT！'
            state = False
        return msg, state


class Password:
    
    posStart = 1
    posMove = 0
    addNum = 1
    
    def __init__(self, text, method, **kwargs):
        self.encodeClass = kwargs.get('encodeClass')
        if not self.encodeClass:
            self.encodeClass = CipherAES()
        self.method = method
        if method not in ['Encrypt', 'Decrypt']:
            raise ValueError('方法错误！')
        if not isinstance(text, str):
            try:
                self.text = str(text)
            except Exception:
                raise ValueError('不支持的密文格式！')
        else:
            self.text = text
        self.key = self.encodeClass.keyGen(kwargs.get('key'))
        if method == 'Decrypt':
            self.iv = text[:8] + text[-8:]
            self.text = text[8:-8]
        else:
            self.iv = self.encodeClass.ivGen(kwargs.get('iv'))
    
    def transform(self):
        if self.method == 'Encrypt':
            return self.encodeClass.encrypt(self.text, self.key, self.iv,
                                            self.posStart, self.posMove,
                                            self.addNum)
        elif self.method == 'Decrypt':
            return self.encodeClass.decrypt(self.text, self.key, self.iv,
                                            self.posStart, self.posMove,
                                            self.addNum)
        else:
            raise ValueError('不支持的密文转换方法！')
