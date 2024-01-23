# Sparklify


## Distirbute
### [ BUILD ]
* Package build
```bash
python3 -m pip install --upgrade build
python3 -m build
```

* result
```
dist/
├── example_package_YOUR_USERNAME_HERE-0.0.1-py3-none-any.whl
└── example_package_YOUR_USERNAME_HERE-0.0.1.tar.gz
```


### [ DEPLOY ]
```bash
python3 -m pip install --upgrade twine
python3 -m twine upload --repository testpypi dist/*
```


After the command completes, you should see output similar to this:

```bash
Uploading distributions to https://test.pypi.org/legacy/
Enter your username: < write "__token__" >
Enter your password: < write token from pypi >
Uploading example_package_YOUR_USERNAME_HERE-0.0.1-py3-none-any.whl
100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 8.2/8.2 kB • 00:01 • ?
Uploading example_package_YOUR_USERNAME_HERE-0.0.1.tar.gz
100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 6.8/6.8 kB • 00:00 • ?
```

Once uploaded, your package should be viewable on TestPyPI; for example: https://test.pypi.org/project/example_package_YOUR_USERNAME_HERE.



### [ INSTALL ] 

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps example-package-YOUR-USERNAME-HERE
```


### [ USE IN ACTION ]

```python
# test.py
from example_package_YOUR_USERNAME_HERE import example
example.add_one(2)
```





