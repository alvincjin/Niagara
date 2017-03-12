package com.alvin.niagara.common

import java.text.SimpleDateFormat

import com.alvin.niagara.model.Post
import com.alvin.niagara.util.Util
import org.scalatest._

/**
 * Created by jinc4 on 6/6/2016.
 */
class UtilTest extends FunSuite with ShouldMatchers{

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  test("ParseXml should return a Post object")({

    val xmlLine =
      """
        |<row Id="4" PostTypeId="1" AcceptedAnswerId="7" CreationDate="2008-07-31T21:42:52.667" Score="305" ViewCount="20324" Body="&lt;p&gt;I want to use a track-bar to change a form's opacity.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;This is my code:&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;decimal trans = trackBar1.Value / 5000;&#xA;this.Opacity = trans;&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;When I try to build it, I get this error:&lt;/p&gt;&#xA;&#xA;&lt;blockquote&gt;&#xA;  &lt;p&gt;Cannot implicitly convert type 'decimal' to 'double'.&lt;/p&gt;&#xA;&lt;/blockquote&gt;&#xA;&#xA;&lt;p&gt;I tried making &lt;code&gt;trans&lt;/code&gt; a &lt;code&gt;double&lt;/code&gt;, but then the control doesn't work. This code has worked fine for me in VB.NET in the past. &lt;/p&gt;&#xA;" OwnerUserId="8" LastEditorUserId="451518" LastEditorDisplayName="Rich B" LastEditDate="2014-07-28T10:02:50.557" LastActivityDate="2014-07-28T10:02:50.557" Title="When setting a form's opacity should I use a decimal or double?" Tags="&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;opacity&gt;" AnswerCount="13" CommentCount="1" FavoriteCount="28" CommunityOwnedDate="2012-10-31T16:42:47.213" />
      """.stripMargin

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val result: Post = Util.parseXml(xmlLine, sdf).get

    val expect = Post(4L, 1, List("c#", "winforms","type-conversion","opacity"), 1217554972667L)

    assert(result === expect)
  })


  test("ParseXml should return a None")({

    val xmlLine =
      """
        |<row Id="4" PostTypeId="11111111111111111111111" AcceptedAnswerId="7" Creationdate="2008-07-31T21:42:52.667" Score="305" ViewCount="20324" Body="&lt;p&gt;I want to use a track-bar to change a form's opacity.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;This is my code:&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;decimal trans = trackBar1.Value / 5000;&#xA;this.Opacity = trans;&#xA;&lt;/code&gt;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;When I try to build it, I get this error:&lt;/p&gt;&#xA;&#xA;&lt;blockquote&gt;&#xA;  &lt;p&gt;Cannot implicitly convert type 'decimal' to 'double'.&lt;/p&gt;&#xA;&lt;/blockquote&gt;&#xA;&#xA;&lt;p&gt;I tried making &lt;code&gt;trans&lt;/code&gt; a &lt;code&gt;double&lt;/code&gt;, but then the control doesn't work. This code has worked fine for me in VB.NET in the past. &lt;/p&gt;&#xA;" OwnerUserId="8" LastEditorUserId="451518" LastEditorDisplayName="Rich B" LastEditDate="2014-07-28T10:02:50.557" LastActivityDate="2014-07-28T10:02:50.557" Title="When setting a form's opacity should I use a decimal or double?" Tags="&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;opacity&gt;" AnswerCount="13" CommentCount="1" FavoriteCount="28" CommunityOwnedDate="2012-10-31T16:42:47.213" />
      """.stripMargin

    val result = Util.parseXml(xmlLine, sdf)

    assert(result === None)
  })

}
