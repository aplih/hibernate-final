package org.toporov.dao;


import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.toporov.domain.Country;

import java.util.List;

public class CountryDAO {
    private final SessionFactory sessionFactory;

    public CountryDAO(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<Country> getAll() {
        try (Session session = sessionFactory.openSession()) {
            return sessionFactory.getCurrentSession().createQuery(
                            "select distinct c from Country c " +
                                    "left join fetch c.cities " +
                                    "left join fetch c.languages", Country.class)
                    .list();
        }
    }
}

