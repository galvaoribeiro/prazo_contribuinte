

with tab_origem as (
select t.nu_guia
--t.da_vencimento, t.id_cpf_cnpj, t.id_receita, t.id_situacao, t.nu_guia_parcela, t.nu_guia, t.va_principal_original
from bi.fato_lanc_arrec t
--where t.id_cpf_cnpj = '12547529000109'
where t.nu_guia = '20100100132982'
and t.id_receita like '1%'
and t.id_receita not like '18%'
    ),
    
/*
tab_redir as (


    select --j.da_vencimento, j.id_cpf_cnpj, j.id_receita, j.id_situacao, j.nu_guia_parcela, j.nu_guia, j.va_principal_original
    distinct
    j.nu_guia
    from bi.fato_lanc_arrec j where j.nu_guia in (

    select h.nu_guia_redir from BI.dm_hist_redir_lanc h
    where substr(h.nu_guia_parcela_origem,1,14) in (select f.nu_guia from tab_origem f)

    )


) */


tab_redir as (

    select substr(h.nu_guia_parcela_origem,1,14) guia_origem, nu_guia_redir from BI.dm_hist_redir_lanc h
    where substr(h.nu_guia_parcela_origem,1,14) in (select f.nu_guia from tab_origem f)

)
    
    
select x.nu_guia, y.nu_guia_redir from tab_origem x left join tab_redir y on x.nu_guia = y.guia_origem

