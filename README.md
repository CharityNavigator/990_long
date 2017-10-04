# 990_long
Generates a long-form version of every field in the IRS 990 eFile dataset based on the Nonprofit Open Data Collective (NOPDC) "Datathon" concordance. The concordance is the work of many separate groups that came together to make sense of the IRS Form 990 eFile dataset. An upcoming NOPDC website will have more information.

# Bootstrap
```
sudo yum -y install tmux htop git
git clone https://github.com/CharityNavigator/990_long
cd 990_long/data
hadoop fs -put concordance_long.csv
hadoop fs -put types.csv
cd ../python
spark-submit simple.py
```

## License

*Copyright (c) 2017 Charity Navigator.*

*Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:*

*The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.*

*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*
