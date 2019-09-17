# modeltest-loadexerciser


## Descrição
Neste projeto temos os fontes utilizados para "exercitar" o software desenvolvido, tomando conta de enviar as requisições e simular o uso real da aplicação uma vez fosse realizada a integração entre o projeto [jModelTest2](https://github.com/mateusaubin/jmodeltest2) e o [protótipo](https://github.com/mateusaubin/modeltest-lambda) desenvolvido. 

Este projeto opera com base em traces de execução do jModelTest2 e submete para a infraestrutura em nuvem as requisições de processamento e coleta os resultados, sendo usado pra disparar os processos, coletar e tabular os resultados.


## Estrutura

* a pasta ```benchmark-results``` contém os resultados das execuções que foram usados para elaborar os dados apresentados na dissertação;
* a pasta ```traces``` contém os arquivos de fonte e de comandos capturados do jModelTest2 e que serviam como input para os comandos disparados por esta aplicação;
* o arquivo ```exec-new.py``` cria os parte dos recursos, controla o workflow e gerencia a coleta de dados ao longo de uma única execução do benchmark;
* o arquivo ```exec-new-helper.sh``` controla a execução de múltiplas execuções do benchmark e era usado para "inicializar" uma VM zerada a cada execução;
* o arquivo ```exec-old-userdata.sh``` inicializa uma VM zerada e executa o jModelTest2, sendo usado para estabelecer o "antes" no comparativo;
* os arquivos ```parser_*.py``` processam os logs de execução para obter os dados estatísticos consolidados;

