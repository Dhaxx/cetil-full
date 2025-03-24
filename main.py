from modules import compras, patrimonio
import utils


def main():
    # compras.base.cadunimedida()
    # compras.base.cadgrupo()
    # compras.base.cadest()
    # compras.base.destino()
    # compras.base.centro_custo()
    # compras.base.fornecedores()

    compras.cotacao.itens()
    utils.armazena_fornecedores()
    compras.cotacao.fcadorc()
    compras.cotacao.vcadorc()

    # patrimonio.limpa_patrimonio()
    # patrimonio.pt_cadtip()
    # patrimonio.pt_cadpatg()
    # patrimonio.pt_cadsit()
    # patrimonio.setores()
    # patrimonio.pt_cadbai()
    # patrimonio.pt_cadpat()
    # patrimonio.pt_movbem()

if __name__ == '__main__':
    main()