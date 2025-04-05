from modules import compras, patrimonio
from utils import armazena_fornecedores

def main():
    # compras.base.cadunimedida()
    # compras.base.cadgrupo()
    # compras.base.cadest()
    # compras.base.destino()
    # compras.base.centro_custo()
    # compras.base.fornecedores()

    # compras.cotacao.itens()
    armazena_fornecedores()
    # compras.cotacao.fcadorc()
    # compras.cotacao.vcadorc()

    # compras.licitacao.cadlic()
    # compras.licitacao.cadlotelic()
    # compras.licitacao.prolic_prolics()
    # compras.licitacao.cadprolic()
    # compras.licitacao.proposta()
    
    # compras.execucao.pedidos()
    # compras.execucao.autorizacao()
    compras.almoxarifado.saldo_inicial()
    compras.almoxarifado.movimento()

    # compras.frotas.motoristas()
    # compras.frotas.veiculos()
    # compras.frotas.abastecimento()

    # patrimonio.limpa_patrimonio()
    # patrimonio.pt_cadtip()
    # patrimonio.pt_cadpatg()
    # patrimonio.pt_cadsit()
    # patrimonio.setores()
    # patrimonio.pt_cadbai()
    patrimonio.pt_cadpat()
    # patrimonio.pt_movbem()

if __name__ == '__main__':
    main()