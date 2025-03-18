from modules import compras, patrimonio


def main():
    patrimonio.limpa_patrimonio()
    patrimonio.pt_cadtip()
    patrimonio.pt_cadpatg()
    patrimonio.pt_cadsit()
    patrimonio.setores()
    patrimonio.pt_cadbai()
    patrimonio.pt_cadpat()
    patrimonio.pt_movbem()

if __name__ == '__main__':
    main()