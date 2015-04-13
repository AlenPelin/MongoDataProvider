/*
    MongoDB DataProvider Sitecore module
    Copyright (C) 2012  Robin Hermanussen
    Copyright (C) 2015  Alen Pelin

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

namespace MongoDataProvider.Data
{
  using System;
  using MongoDB.Bson.Serialization.Attributes;

  [BsonIgnoreExtraElements]
  public class ItemBase
  {
    public Guid ID { get; set; }

    public Guid ParentID { get; set; }
  }
}
