# Source Data: B3 Financial Transactions

This is where you'd input your financial transactions, extracted from the [B3 website](https://www.investidor.b3.com.br/).
Ideally this would come from an API, however it's not so simple to get a key. Instead, you can go the ["Extratos" page](https://www.investidor.b3.com.br/extrato/movimentacao), filter the period you'd like and download an Excel file (yes, it's a xlsx and not a CSV).

You're probably going to have to download multiple files, one from each year, since B3 doesn't allow longer filters. You don't have to unify them manually, just drop them here and the scripts will take care of it for you.

On this repo, I've left a few of my transactions from 2020 as a sample. The quantities you see there are ficticious, but they're realistic enough to get the code to work. You can remove these and add your own. They will automatically be on the gitignore.